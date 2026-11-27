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

package com.datastax.spark.connector.cql

import java.io.IOException
import java.net.InetSocketAddress

import com.datastax.oss.driver.internal.core.metadata.ClientRoutesEndPoint
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.SSLCluster

/** Client routes against a single ScyllaDB node that only accepts TLS connections.
  *
  * With SSL enabled the driver reaches nodes through the routes' `tls_port`, here the node's shard-aware port (which is
  * encrypted like the main port), and ignores routes that publish no `tls_port`.
  */
class ClientRoutesSSLSpec extends SparkCassandraITFlatSpecBase with SSLCluster with ClientRoutesTestSupport {

  override lazy val conn = CassandraConnector(defaultConf)

  private val connectionId = "spark-connector-it-tls"
  private val withoutTlsPortId = "spark-connector-it-no-tls-port"

  // `port` is the contact point's port: a session that used it instead of `tls_port` would not resolve to the route
  private lazy val routes = Seq(
    routeEntry(connectionId, port = Some(9042), tlsPort = Some(shardAwarePort)),
    routeEntry(withoutTlsPortId, port = Some(shardAwarePort), tlsPort = None))

  override def beforeClass: Unit = {
    if (clientRoutesSupported) registerRoutes(routes: _*)
  }

  override def afterClass: Unit = {
    if (clientRoutesSupported) deregisterRoutes(routes: _*)
  }

  "A connector configured with client routes and SSL" should "reach the nodes through the routes' tls_port" in {
    assumeClientRoutes()

    val endPoints = nodeEndPoints(CassandraConnector(routedConf(connectionId)))

    endPoints should not be empty
    all(endPoints) shouldBe a[ClientRoutesEndPoint]
    endPoints.map(_.resolve()) shouldBe Seq(new InetSocketAddress(nodeAddress, shardAwarePort))
  }

  it should "fail on the Spark driver when the routes have no tls_port" in {
    assumeClientRoutes()

    val error = intercept[IOException](CassandraConnector(routedConf(withoutTlsPortId)).withSessionDo(identity))

    error.getMessage should include("have no 'tls_port'")
    error.getMessage should include(withoutTlsPortId)
    error.getCause shouldBe an[IllegalStateException]
  }

  it should "report routes without tls_port even when no local datacenter is set" in {
    assumeClientRoutes()
    val conf = defaultConf.set(CassandraConnectorConf.ClientRoutesEndpointsParam.name, withoutTlsPortId)

    val error = intercept[IOException](CassandraConnector(conf).withSessionDo(identity))

    error.getMessage should include("have no 'tls_port'")
  }
}
