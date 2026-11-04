/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.spark.connector.writer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream}
import java.net.{InetAddress, InetSocketAddress}
import java.util.UUID

import com.datastax.spark.connector.PartitionKeyColumns
import com.datastax.spark.connector.cql.ReplicaTestCluster
import org.apache.spark.TaskContext
import org.apache.spark.util.TaskCompletionListener
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.jdk.CollectionConverters._

case class ReplicaLocatorSpecRow(key: Int)

class ReplicaLocatorSpec extends FlatSpec with Matchers with MockitoSugar {

  private val hostA = UUID.fromString("00000000-0000-0000-0000-00000000000a")
  private val hostB = UUID.fromString("00000000-0000-0000-0000-00000000000b")

  // Named addresses, so that nothing needs a reverse DNS lookup
  private val sharedIp = InetAddress.getByAddress("vpce.example", Array[Byte](10, 0, 0, 5))
  private val ipA = InetAddress.getByAddress("node-a.example", Array[Byte](10, 0, 0, 1))
  private val ipB = InetAddress.getByAddress("node-b.example", Array[Byte](10, 0, 0, 2))

  // Even keys are owned by node A only, odd keys by both nodes
  private def replicasOf(key: Int): Seq[UUID] = if (key % 2 == 0) Seq(hostA) else Seq(hostA, hostB)

  private def sharedIpCluster() = new ReplicaTestCluster(
    Seq(hostA -> new InetSocketAddress(sharedIp, 9042), hostB -> new InetSocketAddress(sharedIp, 9043)), replicasOf)

  private def distinctIpCluster() = new ReplicaTestCluster(
    Seq(hostA -> new InetSocketAddress(ipA, 9042), hostB -> new InetSocketAddress(ipB, 9042)), replicasOf)

  private def locator(cluster: ReplicaTestCluster) =
    ReplicaLocator[ReplicaLocatorSpecRow](cluster.connector, "ks", "tab", PartitionKeyColumns)

  private def rows(keys: Int*): Iterator[ReplicaLocatorSpecRow] = keys.iterator.map(ReplicaLocatorSpecRow)

  /** A Spark task whose completion listeners the test runs, as Spark does when the task completes, fails or is
    * cancelled. */
  private class Task {
    val context: TaskContext = mock[TaskContext]

    def listeners: Seq[TaskCompletionListener] = {
      val captor = ArgumentCaptor.forClass(classOf[TaskCompletionListener])
      verify(context, org.mockito.Mockito.atLeast(0)).addTaskCompletionListener(captor.capture())
      captor.getAllValues.asScala.toSeq
    }

    def complete(): Unit = listeners.foreach(_.onTaskCompletion(context))
  }

  /** The sessions acquired and released since the locator was created. */
  private class Sessions(cluster: ReplicaTestCluster) {
    private val opened = cluster.sessionsOpened.get
    private val closed = cluster.sessionsClosed.get
    def acquired: Int = cluster.sessionsOpened.get - opened
    def released: Int = cluster.sessionsClosed.get - closed
  }

  "ReplicaLocator" should "key rows by the addresses of their replicas, counting replicas that share an address once" in {
    locator(distinctIpCluster()).keyByReplicas(rows(0, 1), None).toList shouldBe Seq(
      Set(ipA) -> ReplicaLocatorSpecRow(0),
      Set(ipA, ipB) -> ReplicaLocatorSpecRow(1))

    locator(sharedIpCluster()).keyByReplicas(rows(0, 1), None).toList shouldBe Seq(
      Set(sharedIp) -> ReplicaLocatorSpecRow(0),
      Set(sharedIp) -> ReplicaLocatorSpecRow(1))
  }

  it should "read the current token map from a session" in {
    val cluster = sharedIpCluster()
    locator(cluster).tokenMap shouldBe cluster.tokenMap
  }

  it should "acquire no session in a task until the first row is requested" in {
    val cluster = sharedIpCluster()
    val replicaLocator = locator(cluster)
    val sessions = new Sessions(cluster)
    val task = new Task

    replicaLocator.keyByReplicas(rows(0, 1), Some(task.context)).hasNext shouldBe true

    sessions.acquired shouldBe 0
    task.listeners shouldBe empty
  }

  it should "hold one session in a task while the rows are consumed, and release it once they are exhausted" in {
    val cluster = sharedIpCluster()
    val replicaLocator = locator(cluster)
    val sessions = new Sessions(cluster)
    val task = new Task

    val keyed = replicaLocator.keyByReplicas(rows(0, 1, 2), Some(task.context))
    keyed.next() shouldBe Set(sharedIp) -> ReplicaLocatorSpecRow(0)
    sessions.acquired shouldBe 1
    sessions.released shouldBe 0
    task.listeners should have size 1

    keyed.toList should have size 2
    sessions.released shouldBe 1

    task.complete()
    sessions.acquired shouldBe 1
    sessions.released shouldBe 1
  }

  it should "release the session when the task completes, fails or is cancelled before the rows are consumed" in {
    val cluster = sharedIpCluster()
    val replicaLocator = locator(cluster)
    val sessions = new Sessions(cluster)
    val task = new Task

    val keyed = replicaLocator.keyByReplicas(rows(0, 1, 2), Some(task.context))
    keyed.next()
    task.complete()

    sessions.released shouldBe 1
    keyed.hasNext shouldBe false
  }

  it should "release the session when the statement cannot be prepared" in {
    val cluster = sharedIpCluster()
    val replicaLocator = locator(cluster)
    val sessions = new Sessions(cluster)
    cluster.prepareFailure = Some(new IllegalStateException("no such table"))

    val keyed = replicaLocator.keyByReplicas(rows(0, 1), Some(new Task().context))

    intercept[IOException](keyed.next()).getMessage should include("no such table")
    sessions.acquired shouldBe 1
    sessions.released shouldBe 1
  }

  it should "release the session when reading the rows fails" in {
    val cluster = sharedIpCluster()
    val replicaLocator = locator(cluster)
    val sessions = new Sessions(cluster)
    val failing = new Iterator[ReplicaLocatorSpecRow] {
      private var served = false
      override def hasNext: Boolean = if (served) throw new IllegalStateException("read") else true
      override def next(): ReplicaLocatorSpecRow = { served = true; ReplicaLocatorSpecRow(0) }
    }

    val keyed = replicaLocator.keyByReplicas(failing, Some(new Task().context))
    keyed.next()

    intercept[IllegalStateException](keyed.hasNext).getMessage shouldBe "read"
    sessions.released shouldBe 1
  }

  it should "key all rows right away outside a Spark task, and release its session before returning" in {
    val cluster = sharedIpCluster()
    val replicaLocator = locator(cluster)
    val sessions = new Sessions(cluster)

    TaskContext.get() shouldBe null
    val keyed = replicaLocator.keyByReplicas(rows(0, 1))

    sessions.acquired shouldBe 1
    sessions.released shouldBe 1
    keyed.toList.map(_._2) shouldBe Seq(ReplicaLocatorSpecRow(0), ReplicaLocatorSpecRow(1))
  }

  it should "serialize without the token map, the nodes or a session" in {
    val cluster = distinctIpCluster()
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    out.writeObject(locator(cluster))
    out.close()

    val in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    val deserialized = in.readObject().asInstanceOf[ReplicaLocator[ReplicaLocatorSpecRow]]

    deserialized.keyByReplicas(rows(1), None).toList shouldBe Seq(Set(ipA, ipB) -> ReplicaLocatorSpecRow(1))
  }
}
