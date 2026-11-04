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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesEndPoint
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.AuthCluster
import org.apache.spark.SparkConf
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import scala.jdk.CollectionConverters._

/** Client routes for a role that is not a superuser, against a ScyllaDB node that checks permissions.
  *
  * ScyllaDB lets every role read `system.local` and `system.peers`, but not `system.client_routes`, which both the
  * driver and the route verification read with the role of the connection.
  */
class ClientRoutesAuthSpec extends SparkCassandraITFlatSpecBase with AuthCluster with ClientRoutesTestSupport {

  /** The superuser of the cluster. */
  override lazy val conn = CassandraConnector(defaultConf)

  private val connectionId = "spark-connector-it-auth"
  // A test role of this suite only, with its name as its password
  private val role = "spark_connector_it_routes"
  // The node only accepts TLS connections, so the driver uses the route's tls_port
  private lazy val route = routeEntry(connectionId, port = Some(9042), tlsPort = Some(shardAwarePort))

  private def roleConf: SparkConf = routedConf(connectionId)
    .set(DefaultAuthConfFactory.UserNameParam.name, role)
    .set(DefaultAuthConfFactory.PasswordParam.name, role)

  override def beforeClass: Unit = {
    if (clientRoutesSupported) {
      conn.withSessionDo { session =>
        session.execute(s"DROP ROLE IF EXISTS $role")
        session.execute(s"CREATE ROLE $role WITH PASSWORD = '$role' AND LOGIN = true")
      }
      registerRoutes(route)
    }
  }

  override def afterClass: Unit = {
    if (clientRoutesSupported) {
      deregisterRoutes(route)
      conn.withSessionDo(_.execute(s"DROP ROLE IF EXISTS $role"))
    }
  }

  "A connector that logs in with a role that is not a superuser" should
    "be told to grant the role SELECT on system.client_routes" in {
    assumeClientRoutes()

    val error = intercept[IOException](CassandraConnector(roleConf).withSessionDo(identity))

    error.getMessage should include("no SELECT permission on <table system.client_routes>")
    error.getMessage should include(ClientRoutesVerifier.GrantHint)
    error.getCause shouldBe an[IllegalStateException]
  }

  it should "reach the node through its route once the role may read system.client_routes" in {
    assumeClientRoutes()
    conn.withSessionDo(_.execute(s"GRANT SELECT ON system.client_routes TO $role"))

    // The node may serve the permissions of the role from its cache for a while. The driver reads the routes once,
    // when the session opens, and the route verification reads them again right after. A session opened while the
    // grant took effect may pass the verification without routes, so each attempt opens a session of its own
    // instead of the one that the connector caches.
    eventually(timeout(30.seconds), interval(1.second)) {
      withNewSession(roleConf) { session =>
        session.execute("SELECT release_version FROM system.local")
        val endPoints = session.getMetadata.getNodes.values.asScala.toSeq.map(_.getEndPoint)
        all(endPoints) shouldBe a[ClientRoutesEndPoint]
        // While the session is open: a closed session no longer resolves routes
        endPoints.map(_.resolve()) shouldBe Seq(new InetSocketAddress(nodeAddress, shardAwarePort))
      }
    }
  }

  /** Runs `f` on a new session, which the connector does not cache, and closes it. */
  private def withNewSession[T](sparkConf: SparkConf)(f: CqlSession => T): T = {
    val conf = CassandraConnectorConf(sparkConf)
    val session = conf.connectionFactory.createSession(conf)
    try f(session) finally session.close()
  }
}
