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

import java.net.{InetAddress, InetSocketAddress}
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.metadata.Node

object DriverUtil {

  //TODO use CqlIdentifier instead? Use implicit conversion to String? To internal string?
  def toName(id: CqlIdentifier): String = id.asInternal()

  def toOption[T](optional: Optional[T]): Option[T] =
    if (optional.isPresent) Some(optional.get()) else None

  def toAddress(node: Node): Option[InetSocketAddress] = {
    node.getEndPoint.resolve() match {
      case address: InetSocketAddress => if (address.isUnresolved) {
        Option(new InetSocketAddress(address.getHostString, address.getPort))
      } else {
        Option(address)
      }
      case _ => toOption(node.getBroadcastAddress)
    }
  }

  /** Returns a function that resolves the address of each node once, for callers that need the address of the same
    * nodes many times.
    *
    * [[toAddress]] resolves the end point of the node on every call. For a node reached through a client route (a
    * private endpoint), that looks the route's address up in DNS and makes a new address object each time, which has
    * to look its host name up again. A resolution that fails is not remembered. */
  def addressResolver(): Node => Option[InetSocketAddress] = {
    val addresses = new ConcurrentHashMap[Node, Option[InetSocketAddress]]()
    node => addresses.computeIfAbsent(node, (n: Node) => toAddress(n))
  }
}
