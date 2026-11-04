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

import java.util.concurrent.{CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class ResourceScopedIteratorSpec extends FlatSpec with Matchers {

  /** Records what happens to the resource, and keeps the release the iterator registers, like a Spark task does. */
  private class Owner(registerNow: Boolean = false) {
    val events: ArrayBuffer[String] = ArrayBuffer.empty
    val acquired = new AtomicInteger
    val released = new AtomicInteger
    @volatile var registered: Option[() => Unit] = None

    def iterator[A](
        data: Iterator[A],
        init: String => A => String = (resource: String) => (a: A) => s"$resource:$a"): ResourceScopedIterator[String, A, String] =
      new ResourceScopedIterator[String, A, String](
        data,
        acquire = () => {
          events.synchronized(events += "acquire")
          s"resource-${acquired.incrementAndGet()}"
        },
        release = _ => {
          events.synchronized(events += "release")
          released.incrementAndGet()
        },
        registerRelease = release => {
          events.synchronized(events += "register")
          registered = Some(release)
          if (registerNow) release()
        },
        init = resource => {
          events.synchronized(events += "init")
          init(resource)
        })

    /** Runs the registered release, as a task completion listener does when a task completes, fails or is
      * cancelled. */
    def completeTask(): Unit = registered.foreach(_.apply())
  }

  "ResourceScopedIterator" should "acquire nothing until the first element is requested" in {
    val owner = new Owner
    val unconsumed = owner.iterator(Iterator(1, 2))
    unconsumed.hasNext shouldBe true

    val noData = owner.iterator(Iterator.empty)
    noData.hasNext shouldBe false
    noData.toList shouldBe empty

    owner.completeTask()
    owner.acquired.get shouldBe 0
    owner.released.get shouldBe 0
    owner.registered shouldBe None
  }

  it should "register the release right after acquiring, and release once when the data is exhausted" in {
    val owner = new Owner
    val iterator = owner.iterator(Iterator(1, 2, 3))

    iterator.next() shouldBe "resource-1:1"
    owner.events shouldBe Seq("acquire", "register", "init")
    owner.released.get shouldBe 0

    iterator.toList shouldBe Seq("resource-1:2", "resource-1:3")
    owner.released.get shouldBe 1

    iterator.hasNext shouldBe false
    iterator.close()
    owner.completeTask()
    owner.acquired.get shouldBe 1
    owner.released.get shouldBe 1
  }

  it should "release once when the task completes before the data is consumed" in {
    val owner = new Owner
    val iterator = owner.iterator(Iterator(1, 2, 3))
    iterator.next()

    owner.completeTask()
    owner.released.get shouldBe 1
    iterator.hasNext shouldBe false
    an[IllegalStateException] should be thrownBy iterator.next()

    owner.completeTask()
    owner.released.get shouldBe 1
  }

  it should "release once when deriving the mapping function fails" in {
    val owner = new Owner
    val iterator = owner.iterator(Iterator(1, 2), init = _ => throw new IllegalStateException("prepare failed"))

    intercept[IllegalStateException](iterator.next()).getMessage shouldBe "prepare failed"
    owner.released.get shouldBe 1
    iterator.hasNext shouldBe false
    owner.completeTask()
    owner.released.get shouldBe 1
  }

  it should "release once when mapping an element or reading the data fails" in {
    val failingMap = new Owner
    val mapping = failingMap.iterator(Iterator(1, 2), init = _ => (a: Int) =>
      if (a == 2) throw new IllegalArgumentException("bad row") else a.toString)
    mapping.next() shouldBe "1"
    intercept[IllegalArgumentException](mapping.next()).getMessage shouldBe "bad row"
    failingMap.released.get shouldBe 1

    val failingData = new Owner
    val reading = failingData.iterator(new Iterator[Int] {
      private var served = false
      override def hasNext: Boolean = if (served) throw new IllegalStateException("read") else true
      override def next(): Int = { served = true; 1 }
    })
    reading.next()
    intercept[IllegalStateException](reading.hasNext).getMessage shouldBe "read"
    failingData.released.get shouldBe 1
    failingData.completeTask()
    failingData.released.get shouldBe 1
  }

  it should "release right away and fail when the release runs as it is registered" in {
    // A Spark task that has completed already runs a new completion listener right away
    val owner = new Owner(registerNow = true)
    val iterator = owner.iterator(Iterator(1, 2))

    an[IllegalStateException] should be thrownBy iterator.next()
    owner.events shouldBe Seq("acquire", "register", "release")
    owner.released.get shouldBe 1
  }

  it should "release once when the task completes while the data is being exhausted" in {
    val pool = Executors.newFixedThreadPool(2)
    try {
      for (_ <- 1 to 200) {
        val owner = new Owner
        val iterator = owner.iterator(Iterator(1, 2))
        iterator.next()
        val start = new CyclicBarrier(2)
        val done = new CountDownLatch(2)
        pool.submit(new Runnable {
          override def run(): Unit = try { start.await(); iterator.foreach(_ => ()) } finally done.countDown()
        })
        pool.submit(new Runnable {
          override def run(): Unit = try { start.await(); owner.completeTask() } finally done.countDown()
        })
        done.await(10, TimeUnit.SECONDS) shouldBe true
        owner.released.get shouldBe 1
      }
    } finally pool.shutdownNow()
  }
}
