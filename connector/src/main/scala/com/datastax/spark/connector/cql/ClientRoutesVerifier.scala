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

import java.util.UUID

import com.datastax.oss.driver.api.core.{AllNodesFailedException, CqlSession}
import com.datastax.oss.driver.api.core.cql.{Row, SimpleStatement}
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException
import com.datastax.spark.connector.cql.CassandraConnectorConf.{ClientRoutesEndpointsParam, LocalDCParam,
  SSLEnabledParam}
import com.datastax.spark.connector.util.Logging
import org.apache.spark.SparkEnv

import scala.jdk.CollectionConverters._

/** Fails fast when the configured client routes do not cover the local datacenter.
  *
  * A connection ID belongs to one availability zone of one datacenter, so the routes of several connection IDs
  * together cover the local datacenter. A node covered by several of them only needs one usable route: the driver
  * keeps one route per node.
  *
  * The driver still opens a session when a connection ID matches no route, and it skips every route it cannot use:
  * one without an address (neither the connection's address override nor a published one) or with an empty one, and
  * one without the port it needs (`tls_port` when SSL is enabled, `port` otherwise) or with a port outside 1-65535.
  * A node without a usable route falls back to its own address, which is not reachable through a private endpoint,
  * and the failure only shows up later as connection errors on executors. Checking the routes when the Spark driver
  * opens its session turns such mistakes into an immediate configuration error.
  *
  * Servers without client routes support need no check here: the driver refuses to open the session, because the
  * server rejects the `CLIENT_ROUTES_CHANGE` event registration.
  */
private[cql] object ClientRoutesVerifier extends Logging {

  private val RoutesQuery =
    "SELECT connection_id, host_id, address, port, tls_port FROM system.client_routes " +
      "WHERE connection_id IN ? ALLOW FILTERING"

  // Every role may read system.local and system.peers, but only a superuser or a granted role system.client_routes
  private[cql] val GrantHint =
    "a role that is not a superuser needs GRANT SELECT ON system.client_routes TO <the role of " +
      s"${DefaultAuthConfFactory.UserNameParam.name}>"

  final case class Route(
    connectionId: String,
    hostId: UUID,
    address: Option[String],
    port: Option[Int],
    tlsPort: Option[Int])

  private val ValidPorts = 1 to 65535

  /** Why the driver skips a route. */
  private sealed trait Unusable
  private case object NoAddress extends Unusable
  private case object NoPort extends Unusable
  private case object InvalidPort extends Unusable

  /** Executor ID of the Spark driver, `SparkContext.DRIVER_IDENTIFIER` is private to Spark. */
  private[cql] val SparkDriverExecutorId = "driver"

  /** Executors skip the check: the Spark driver already checked the same configuration. */
  def runsOnSparkDriver: Boolean =
    Option(SparkEnv.get).forall(_.executorId == SparkDriverExecutorId)

  /** @throws IllegalStateException if a node of `localDC` has no usable route */
  def verify(session: CqlSession, routes: ClientRoutesConf, localDC: String): Unit = {
    // Same rule as the driver: routes use `tls_port` exactly when an SSL engine is configured.
    val useTls = session.getContext.getSslEngineFactory.isPresent
    val nodes = session.getMetadata.getNodes.values.asScala.toSeq
    val localHostIds = nodes.filter(_.getDatacenter == localDC).flatMap(node => Option(node.getHostId)).toSet
    val knownDCs = nodes.flatMap(node => Option(node.getDatacenter)).toSet
    // The query only goes to nodes of the local datacenter: without any, evaluate reports the datacenter instead
    val published = if (localHostIds.isEmpty) Nil else fetchRoutes(session, routes.connectionIds, localDC, useTls)
    val addressOverrides = routes.endpoints.collect { case ClientRouteEndpointConf(id, Some(addr)) => id -> addr }.toMap
    evaluate(localDC, routes.connectionIds, published, useTls, localHostIds, knownDCs, addressOverrides) match {
      case Left(error) =>
        throw new IllegalStateException(error)
      case Right(warnings) =>
        warnings.foreach(logWarning(_))
        logInfo(s"Client routes cover all ${localHostIds.size} node(s) of datacenter '$localDC'.")
    }
  }

  /** @throws IllegalStateException if no node of `localDC` can be reached, typically because none of them has a usable
    *                               route: the nodes then fall back to their own addresses; or if the role of the
    *                               connection may not read `system.client_routes` */
  private def fetchRoutes(
    session: CqlSession,
    connectionIds: Seq[String],
    localDC: String,
    useTls: Boolean): Seq[Route] = {
    val rows =
      try session.execute(SimpleStatement.newInstance(RoutesQuery, connectionIds.asJava)).all().asScala.toSeq
      catch {
        case e: AllNodesFailedException =>
          throw new IllegalStateException(
            s"Client routes: no node of the local datacenter '$localDC' could be reached to read " +
              "system.client_routes. This usually means that none of them has a usable client route: check that " +
              s"${ClientRoutesEndpointsParam.name} lists the connection IDs that serve this datacenter " +
              s"(configured: ${connectionIds.mkString(", ")}), that their routes publish an address and a " +
              s"'${if (useTls) "tls_port" else "port"}', that ${LocalDCParam.name} names their datacenter, and " +
              s"that the role may read system.client_routes ($GrantHint).", e)
        // The driver reads the routes with the same role: it then logs the error and routes no node
        case e: UnauthorizedException =>
          throw new IllegalStateException(
            s"Client routes: ${e.getMessage}. The driver reads the routes with the same role, so it cannot route " +
              s"any node either: $GrantHint.", e)
      }
    rows.map { row =>
      Route(
        row.getString("connection_id"),
        row.getUuid("host_id"),
        Option(row.getString("address")),
        optionalInt(row, "port"),
        optionalInt(row, "tls_port"))
    }
  }

  private def optionalInt(row: Row, column: String): Option[Int] =
    if (row.isNull(column)) None else Some(row.getInt(column))

  /** Applies the driver's rules to the routes of the configured connection IDs: a route is usable when it has an
    * address (the connection's override, otherwise the published one) that is not empty, and the port the connection
    * uses within 1-65535. The driver skips the other routes, each on its own.
    *
    * @param addressOverrides the address overrides of the configured connection IDs
    * @return an error when a node of `localDC` has no usable route, otherwise warnings about connection IDs that match
    *         no route at all (typically a typo in a remote datacenter's connection ID)
    */
  def evaluate(
    localDC: String,
    connectionIds: Seq[String],
    routes: Seq[Route],
    useTls: Boolean,
    localHostIds: Set[UUID],
    knownDCs: Set[String],
    addressOverrides: Map[String, String] = Map.empty): Either[String, Seq[String]] = {

    val portColumn = if (useTls) "tls_port" else "port"
    def unusable(route: Route): Option[Unusable] = {
      val address = addressOverrides.get(route.connectionId).orElse(route.address)
      val port = if (useTls) route.tlsPort else route.port
      if (address.forall(_.isEmpty)) Some(NoAddress)
      else if (port.isEmpty) Some(NoPort)
      else if (!port.exists(ValidPorts.contains)) Some(InvalidPort)
      else None
    }
    val missing = localHostIds -- routes.filter(unusable(_).isEmpty).map(_.hostId)
    val routesByHost = routes.groupBy(_.hostId)
    def countWith(problem: Unusable): Int =
      missing.count(host => routesByHost.get(host).exists(_.exists(unusable(_).contains(problem))))
    if (localHostIds.isEmpty) {
      Left(s"Client routes: no nodes found in the local datacenter '$localDC'. " +
        s"Known datacenters: ${knownDCs.toSeq.sorted.mkString(", ")}.")
    } else if (missing.nonEmpty) {
      val unknownConnectionHint =
        if (missing.forall(routesByHost.contains)) Nil
        else Seq(s"Check that ${ClientRoutesEndpointsParam.name} lists every connection ID that serves this " +
          "datacenter (a datacenter can have one per availability zone).")
      val hints = Seq(
        NoAddress -> ("no address, or an empty one: set the address of their connection ID with connectionId=address " +
          s"in ${ClientRoutesEndpointsParam.name}."),
        NoPort -> (s"no '$portColumn', which the driver needs when TLS is ${if (useTls) "enabled" else "disabled"} " +
          s"(${SSLEnabledParam.name})."),
        InvalidPort -> s"a '$portColumn' outside 1-65535.")
        .map { case (problem, text) => (countWith(problem), text) }
        .collect { case (count, text) if count > 0 => s"The routes of $count of them have $text" }
      val summary = s"Client routes: ${missing.size} of ${localHostIds.size} node(s) in the local datacenter " +
        s"'$localDC' have no usable client route for connection ID(s) ${connectionIds.mkString(", ")} " +
        s"(host IDs: ${missing.toSeq.map(_.toString).sorted.mkString(", ")})."
      Left((summary +: (unknownConnectionHint ++ hints)).mkString(" "))
    } else {
      val idsWithRoutes = routes.map(_.connectionId).toSet
      Right(connectionIds
        .filterNot(idsWithRoutes)
        .map(id => s"Client routes: connection ID '$id' has no entries in system.client_routes."))
    }
  }
}
