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

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.metadata.EndPoint
import com.datastax.spark.connector.SparkCassandraITSpecBase
import com.datastax.spark.connector.cql.CassandraConnectorConf.{ClientRoutesEndpointsParam, LocalDCParam}
import com.datastax.spark.connector.util.DriverUtil
import org.apache.spark.SparkConf

import scala.jdk.CollectionConverters._

/** Registers ScyllaDB client routes for integration tests.
  *
  * Routes map a test node to one of its own ports, which stands in for the `address:port` that a private endpoint
  * publishes for a node. They are registered through the REST API (port 10000) of the first node, which must be
  * reachable on the node address. Client routes need ScyllaDB 2026.1 or newer; tests call [[assumeClientRoutes]] to be
  * skipped otherwise.
  */
trait ClientRoutesTestSupport { this: SparkCassandraITSpecBase =>

  /** Scylla serves CQL on this port too, with the same encryption as the main native transport port. */
  protected val shardAwarePort = 19042

  protected lazy val nodeAddress: String = cluster.addresses.head.getAddress.getHostAddress

  private lazy val clientRoutesApi = URI.create(s"http://$nodeAddress:10000/v2/client-routes")
  // Bounds every REST call, so that an unresponsive REST API fails the setup and teardown of the suites instead of
  // blocking them
  private val restApiTimeout = Duration.ofSeconds(30)
  private lazy val http = HttpClient.newBuilder().connectTimeout(restApiTimeout).build()

  protected lazy val clientRoutesSupported: Boolean = isScylla && conn.withSessionDo { session =>
    session.execute(
      "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'system' AND table_name = 'client_routes'"
    ).one() != null
  }

  protected def assumeClientRoutes(): Unit =
    assume(clientRoutesSupported, "client routes need ScyllaDB 2026.1 or newer")

  protected lazy val datacenter: String = conn.withSessionDo(testDatacenter)

  /** Host ID and address of every node of the cluster, in the order of the cluster's addresses. */
  protected lazy val nodes: Seq[(UUID, String)] = conn.withSessionDo { session =>
    val hostIds = session.getMetadata.getNodes.values.asScala
      .map(node => DriverUtil.toAddress(node).get.getAddress.getHostAddress -> node.getHostId)
      .toMap
    cluster.addresses.map(_.getAddress.getHostAddress).map(address => hostIds(address) -> address)
  }

  private lazy val hostId: UUID = nodes.head._1

  /** A route of a node, by default the first one; `None` leaves the column empty. */
  protected def routeEntry(
    connectionId: String,
    port: Option[Int],
    tlsPort: Option[Int],
    address: String = nodeAddress,
    hostId: UUID = this.hostId): String = {
    val ports = Seq("port" -> port, "tls_port" -> tlsPort).collect { case (column, Some(p)) => s""""$column":$p""" }
    (s""""connection_id":"$connectionId"""" +: s""""host_id":"$hostId"""" +: s""""address":"$address"""" +: ports)
      .mkString("{", ",", "}")
  }

  /** Publishes the given routes and waits until the node serves them. */
  protected def registerRoutes(entries: String*): Unit = {
    callClientRoutesApi("POST", entries.mkString("[", ",", "]"))
    val connectionIds = entries.map(entry => """"connection_id":"([^"]+)"""".r.findFirstMatchIn(entry).get.group(1))
    connectionIds.distinct.foreach(awaitRoutePublished)
  }

  protected def deregisterRoutes(entries: String*): Unit =
    callClientRoutesApi("DELETE", entries.mkString("[", ",", "]"))

  /** Connection settings that differ from the default cluster ones only by SparkConf client routes keys. */
  protected def routedConf(connectionIds: String): SparkConf =
    defaultConf
      .set(ClientRoutesEndpointsParam.name, connectionIds)
      .set(LocalDCParam.name, datacenter)

  /** Opens a session, runs a query and returns the end points of the known nodes. */
  protected def nodeEndPoints(connector: CassandraConnector): Seq[EndPoint] =
    connector.withSessionDo { session =>
      session.execute("SELECT release_version FROM system.local")
      session.getMetadata.getNodes.values.asScala.toSeq.map(_.getEndPoint)
    }

  private def callClientRoutesApi(method: String, body: String): Unit = {
    val request = HttpRequest.newBuilder(clientRoutesApi)
      .timeout(restApiTimeout)
      .header("Content-Type", "application/json")
      .method(method, HttpRequest.BodyPublishers.ofString(body))
      .build()
    // The request timeout stops once the response headers arrive: waiting for the whole exchange bounds the body too
    val pending = http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
    val response =
      try pending.get(restApiTimeout.toMillis, TimeUnit.MILLISECONDS)
      catch {
        case _: TimeoutException =>
          pending.cancel(true)
          fail(s"$method $clientRoutesApi did not complete within $restApiTimeout")
        case e: ExecutionException => throw e.getCause
      }
    withClue(s"$method $clientRoutesApi returned ${response.statusCode()}: ${response.body()}") {
      response.statusCode() / 100 shouldBe 2
    }
  }

  private def awaitRoutePublished(connectionId: String): Unit = {
    val query = SimpleStatement.newInstance(
      "SELECT host_id FROM system.client_routes WHERE connection_id = ? ALLOW FILTERING", connectionId)
    val deadline = System.nanoTime() + 30L * 1000 * 1000 * 1000
    while (conn.withSessionDo(_.execute(query).one()) == null) {
      if (System.nanoTime() > deadline) fail(s"The route of $connectionId was not published in time")
      Thread.sleep(500)
    }
  }
}
