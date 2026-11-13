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

package com.datastax.spark.connector.util

import java.io.UncheckedIOException
import java.net.{InetSocketAddress, UnknownHostException}

import com.datastax.oss.driver.api.core.metadata.{EndPoint, Node}
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class DriverUtilSpec extends FlatSpec with Matchers with MockitoSugar {

  private def nodeWith(endPoint: EndPoint): Node = {
    val node = mock[Node]
    when(node.getEndPoint).thenReturn(endPoint)
    node
  }

  /** An end point that makes a new address on every call, like the end point of a client route does. */
  private def endPointAt(address: String): EndPoint = {
    val endPoint = mock[EndPoint]
    when(endPoint.resolve()).thenAnswer(_ => new InetSocketAddress(address, 9042))
    endPoint
  }

  "DriverUtil.addressResolver" should "resolve the end point of a node once" in {
    val endPoint = endPointAt("10.1.0.1")
    val node = nodeWith(endPoint)
    val addressOf = DriverUtil.addressResolver()

    val first = addressOf(node)

    first shouldBe Some(new InetSocketAddress("10.1.0.1", 9042))
    addressOf(node).get should be theSameInstanceAs first.get
    verify(endPoint, times(1)).resolve()
  }

  it should "resolve every node on its own" in {
    val addressOf = DriverUtil.addressResolver()

    addressOf(nodeWith(endPointAt("10.1.0.1"))) shouldBe Some(new InetSocketAddress("10.1.0.1", 9042))
    addressOf(nodeWith(endPointAt("10.1.0.2"))) shouldBe Some(new InetSocketAddress("10.1.0.2", 9042))
  }

  it should "not remember a resolution that failed" in {
    val endPoint = mock[EndPoint]
    when(endPoint.resolve())
      .thenThrow(new UncheckedIOException(new UnknownHostException("vpce.invalid")))
      .thenReturn(new InetSocketAddress("10.1.0.1", 9042))
    val node = nodeWith(endPoint)
    val addressOf = DriverUtil.addressResolver()

    intercept[UncheckedIOException](addressOf(node))

    addressOf(node) shouldBe Some(new InetSocketAddress("10.1.0.1", 9042))
  }

  it should "resolve again in a new resolver" in {
    val endPoint = endPointAt("10.1.0.1")
    val node = nodeWith(endPoint)

    DriverUtil.addressResolver()(node)
    DriverUtil.addressResolver()(node)

    verify(endPoint, times(2)).resolve()
  }
}
