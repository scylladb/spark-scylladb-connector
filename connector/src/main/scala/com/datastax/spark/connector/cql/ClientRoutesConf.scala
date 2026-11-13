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

import com.datastax.oss.driver.api.core.config.{ClientRouteProxy, ClientRoutesConfig}

import scala.util.Try

/** One private endpoint connection of a ScyllaDB Cloud cluster.
  *
  * @param connectionId   connection ID of the private endpoint, as shown in the ScyllaDB Cloud console
  * @param connectionAddr optional hostname or IP address (without port) that replaces the address published in
  *                       `system.client_routes` for this connection ID, e.g. the IP address of the endpoint when
  *                       its DNS name does not resolve inside the client network
  */
final case class ClientRouteEndpointConf(connectionId: String, connectionAddr: Option[String] = None)

/** Connects through ScyllaDB Cloud private endpoints (AWS PrivateLink, GCP Private Service Connect) using the
  * driver's client routes support.
  *
  * The driver reads `system.client_routes` for the configured connection IDs and reaches every node through the
  * `address:port` published for it (`address:tls_port` when SSL is enabled), so neither the Spark driver nor the
  * executors need to reach node addresses directly. Requires ScyllaDB 2026.1 or newer.
  *
  * This is a plain, serializable description of the driver's `ClientRoutesConfig`, which is not serializable and
  * therefore built only when a session is created.
  *
  * @param endpoints      private endpoint connections the driver may use. A connection ID belongs to one availability
  *                       zone of one datacenter, so a datacenter can have several; together, they must route every
  *                       node of the local datacenter
  * @param shardAwareness enable the driver's advanced shard awareness, which opens each connection from a client
  *                       source port that targets a shard. Off by default on the client side, as in the driver.
  *                       ScyllaDB Cloud private endpoints support it, so enable it there; leave it off behind a proxy
  *                       that does not preserve the client source port, such as one without Proxy Protocol v2. Basic
  *                       shard awareness, sending each request to a connection of the shard that owns the data, works
  *                       either way
  * @param verify         check, when the Spark driver opens a session, that every node of the local datacenter has
  *                       a usable route, see [[ClientRoutesVerifier]]
  * @param nativeTransportPort the native transport (CQL) port of the nodes themselves, not of the private endpoint.
  *                       ScyllaDB does not publish it in its system tables, and the driver needs it to match the
  *                       cluster's topology and status events to nodes. Change it only when the nodes do not listen
  *                       on 9042
  */
final case class ClientRoutesConf(
  endpoints: Seq[ClientRouteEndpointConf],
  shardAwareness: Boolean = false,
  verify: Boolean = true,
  nativeTransportPort: Int = ClientRoutesConfig.DEFAULT_NATIVE_TRANSPORT_PORT) {

  locally {
    val errors =
      ClientRoutesConf.validate(endpoints) ++ ClientRoutesConf.validateNativeTransportPort(nativeTransportPort)
    require(errors.isEmpty, s"Invalid client routes: ${errors.mkString(" ")}")
  }

  def connectionIds: Seq[String] = endpoints.map(_.connectionId)

  /** Builds the driver configuration. */
  def toDriver: ClientRoutesConfig =
    endpoints
      .foldLeft(ClientRoutesConfig.builder()) { (builder, endpoint) =>
        builder.addEndpoint(new ClientRouteProxy(endpoint.connectionId, endpoint.connectionAddr.orNull))
      }
      .withShardAwareness(shardAwareness)
      .withNativeTransportPort(nativeTransportPort)
      .build()
}

object ClientRoutesConf {

  // The driver checks an address only together with a connection ID: this one lets it check the address alone
  private val ValidConnectionId = "connection-id"

  /** Returns a description of every problem of the given endpoints, the address checks are delegated to the
    * driver's [[ClientRouteProxy]]. */
  private[cql] def validate(endpoints: Seq[ClientRouteEndpointConf]): Seq[String] =
    validateNumbered(endpoints.zipWithIndex.map { case (endpoint, i) => (s"endpoint ${i + 1}", endpoint) })

  /** Same as [[validate]], with the name of every endpoint in the messages. */
  private def validateNumbered(endpoints: Seq[(String, ClientRouteEndpointConf)]): Seq[String] = {
    val endpointErrors = endpoints.flatMap { case (field, endpoint) =>
      val idError =
        if (endpoint.connectionId == null || endpoint.connectionId.trim.isEmpty)
          Some("the connection ID must not be empty")
        else None
      val addressError = endpoint.connectionAddr.flatMap { addr =>
        if (addr == null || addr.trim.isEmpty) Some("the connection address must not be empty when set")
        else Try(new ClientRouteProxy(ValidConnectionId, addr)).failed.toOption.map(_.getMessage)
      }
      Seq(idError, addressError).flatten.map(error => s"$field: $error.")
    }
    val duplicateIds = endpoints
      .map { case (_, endpoint) => endpoint.connectionId }
      .filter(id => id != null && id.trim.nonEmpty)
      .groupBy(identity)
      .collect { case (id, occurrences) if occurrences.size > 1 => id }
      .toSeq
      .sorted
    val noEndpoints = if (endpoints.isEmpty) Seq("at least one connection ID is required.") else Nil
    val duplicates =
      if (duplicateIds.nonEmpty) Seq(s"duplicate connection ID(s): ${duplicateIds.mkString(", ")}.") else Nil
    noEndpoints ++ endpointErrors ++ duplicates
  }

  private[cql] def validateNativeTransportPort(port: Int): Seq[String] =
    if (port >= 1 && port <= 65535) Nil
    else Seq(s"the native transport port must be between 1 and 65535, got $port.")

  /** Parses a comma-separated list of `connectionId` or `connectionId=connectionAddr` entries.
    *
    * @return the endpoints, or a description of every problem found, which refers to the entries by position
    */
  def parseEndpoints(value: String): Either[Seq[String], Seq[ClientRouteEndpointConf]] = {
    val entries = value.split(",", -1).map(_.trim).toList
    if (entries.forall(_.isEmpty)) {
      Left(Seq("at least one connection ID is required."))
    } else {
      val parsed = entries.zipWithIndex.map { case (entry, i) =>
        val field = s"entry ${i + 1}"
        entry.split("=", 2) match {
          case _ if entry.isEmpty => Left(s"$field is empty.")
          case Array(id) => Right(field -> ClientRouteEndpointConf(id.trim))
          case Array(id, addr) => Right(field -> ClientRouteEndpointConf(id.trim, Some(addr.trim)))
        }
      }
      val endpoints = parsed.collect { case Right(endpoint) => endpoint }
      // An empty entry does not keep the other entries from being checked
      val errors = parsed.collect { case Left(error) => error } ++ validateNumbered(endpoints)
      if (errors.nonEmpty) Left(errors) else Right(endpoints.map { case (_, endpoint) => endpoint })
    }
  }
}
