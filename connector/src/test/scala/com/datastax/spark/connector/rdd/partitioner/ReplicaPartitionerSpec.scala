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

package com.datastax.spark.connector.rdd.partitioner

import java.net.{InetAddress, InetSocketAddress}
import java.util.UUID

import com.datastax.spark.connector.PartitionKeyColumns
import com.datastax.spark.connector.cql.ReplicaTestCluster
import org.apache.spark.Partition
import org.scalatest.{FlatSpec, Matchers}

case class ReplicaPartitionerSpecRow(key: Int)

/** Nodes that share an IP address, as nodes behind a private endpoint can, through the connector's mapping of hosts
  * (`hostsById`) and the replica partitioner. The cluster is described by mocked driver metadata: these tests do not
  * cover connecting to nodes that share an address. */
class ReplicaPartitionerSpec extends FlatSpec with Matchers {

  private val hostA = UUID.fromString("00000000-0000-0000-0000-00000000000a")
  private val hostB = UUID.fromString("00000000-0000-0000-0000-00000000000b")
  private val remoteHost = UUID.fromString("00000000-0000-0000-0000-0000000000cc")

  // Named addresses, so that the location hints need no reverse DNS lookup
  private val sharedIp = InetAddress.getByAddress("vpce.example", Array[Byte](10, 0, 0, 5))
  private val ipA = InetAddress.getByAddress("node-a.example", Array[Byte](10, 0, 0, 1))
  private val ipB = InetAddress.getByAddress("node-b.example", Array[Byte](10, 0, 0, 2))

  private val sharedA = hostA -> new InetSocketAddress(sharedIp, 9042)
  private val sharedB = hostB -> new InetSocketAddress(sharedIp, 9043)

  private val PartitionsPerHost = 3

  private val onlyA = (0 until 100).toList
  private val onlyB = (100 until 200).toList
  private val both = (200 until 300).toList
  private val otherDC = (1000 until 1100).toList

  private def replicasOf(key: Int): Seq[UUID] =
    if (onlyA.contains(key)) Seq(hostA)
    else if (onlyB.contains(key)) Seq(hostB)
    else if (both.contains(key)) Seq(hostA, hostB)
    else Seq(remoteHost)

  private def cluster(nodes: (UUID, InetSocketAddress)*) =
    new ReplicaTestCluster(nodes, replicasOf, remoteNodes = Seq(remoteHost -> new InetSocketAddress(ipA, 9044)))

  private def partitioner(cluster: ReplicaTestCluster) = new ReplicaPartitioner[ReplicaPartitionerSpecRow](
    "tab", "ks", PartitionsPerHost, PartitionKeyColumns, cluster.connector)

  private def partitionsOf(partitioner: ReplicaPartitioner[ReplicaPartitionerSpecRow], keys: Seq[Int]): Set[Int] =
    keys.map(key => partitioner.getPartition(ReplicaPartitionerSpecRow(key))).toSet

  private def locationHints(partitioner: ReplicaPartitioner[ReplicaPartitionerSpecRow], partition: Int): Set[String] =
    partitioner.getEndpointPartition(new Partition { override def index: Int = partition }).endpoints.toSet

  "ReplicaPartitioner" should "keep nodes that share an IP address apart, by host ID" in {
    val sharedIpCluster = cluster(sharedA, sharedB)
    val replicaPartitioner = partitioner(sharedIpCluster)

    sharedIpCluster.connector.hostsById shouldBe Map(sharedA, sharedB)
    replicaPartitioner.numPartitions shouldBe 2 * PartitionsPerHost

    val partitionsOfA = partitionsOf(replicaPartitioner, onlyA)
    val partitionsOfB = partitionsOf(replicaPartitioner, onlyB)
    partitionsOfA should have size PartitionsPerHost
    partitionsOfB should have size PartitionsPerHost
    partitionsOfA intersect partitionsOfB shouldBe empty
    partitionsOfA ++ partitionsOfB shouldBe (0 until replicaPartitioner.numPartitions).toSet
    partitionsOf(replicaPartitioner, both) should contain allElementsOf partitionsOfA ++ partitionsOfB

    // Every partition has the same location hints, the shared address, and still belongs to one node only
    (0 until replicaPartitioner.numPartitions).map(locationHints(replicaPartitioner, _)).toSet shouldBe
      Set(Set("10.0.0.5", "vpce.example"))
  }

  it should "put a row whose replicas are all in another datacenter into a partition of the local datacenter" in {
    val replicaPartitioner = partitioner(cluster(sharedA, sharedB))

    partitionsOf(replicaPartitioner, otherDC) should not be empty
    partitionsOf(replicaPartitioner, otherDC).foreach { partition =>
      partition should (be >= 0 and be < replicaPartitioner.numPartitions)
    }
  }

  it should "give each node the same partitions whatever the order of the nodes in the metadata" in {
    val inOrder = partitioner(cluster(sharedA, sharedB))
    val reversed = partitioner(cluster(sharedB, sharedA))
    val rows = (onlyA ++ onlyB ++ both ++ otherDC).map(ReplicaPartitionerSpecRow)

    rows.map(reversed.getPartition) shouldBe rows.map(inOrder.getPartition)
    (0 until inOrder.numPartitions).map(locationHints(reversed, _)) shouldBe
      (0 until inOrder.numPartitions).map(locationHints(inOrder, _))
  }

  it should "give a single node all the partitions" in {
    val singleNode = cluster(sharedA)
    val replicaPartitioner = partitioner(singleNode)

    singleNode.connector.hostsById shouldBe Map(sharedA)
    replicaPartitioner.numPartitions shouldBe PartitionsPerHost
    partitionsOf(replicaPartitioner, onlyA ++ onlyB ++ both ++ otherDC) shouldBe (0 until PartitionsPerHost).toSet
  }

  it should "give the partitions of nodes with their own IP addresses the location hints of their node" in {
    val distinctIps = cluster(hostA -> new InetSocketAddress(ipA, 9042), hostB -> new InetSocketAddress(ipB, 9042))
    val replicaPartitioner = partitioner(distinctIps)

    replicaPartitioner.numPartitions shouldBe 2 * PartitionsPerHost
    val partitionsOfA = partitionsOf(replicaPartitioner, onlyA)
    val partitionsOfB = partitionsOf(replicaPartitioner, onlyB)
    partitionsOfA intersect partitionsOfB shouldBe empty
    partitionsOfA.map(locationHints(replicaPartitioner, _)) shouldBe Set(Set("10.0.0.1", "node-a.example"))
    partitionsOfB.map(locationHints(replicaPartitioner, _)) shouldBe Set(Set("10.0.0.2", "node-b.example"))
  }
}
