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

import java.util.{Optional, UUID}

import com.datastax.oss.driver.api.core.{CqlSession, NoNodeAvailableException}
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.oss.driver.api.core.metadata.{Metadata, Node}
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException
import com.datastax.spark.connector.cql.CassandraConnectorConf._
import com.datastax.spark.connector.cql.ClientRoutesVerifier.Route
import org.apache.spark.{SparkConf, SparkEnv}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.{FlatSpec, Matchers}

import scala.jdk.CollectionConverters._

class ClientRoutesVerifierSpec extends FlatSpec with Matchers {

  private val dc1Node1 = UUID.fromString("00000000-0000-0000-0000-000000000011")
  private val dc1Node2 = UUID.fromString("00000000-0000-0000-0000-000000000012")
  private val dc1Node3 = UUID.fromString("00000000-0000-0000-0000-000000000013")
  private val dc2Node1 = UUID.fromString("00000000-0000-0000-0000-000000000021")

  private val IdsHint = s"Check that ${ClientRoutesEndpointsParam.name} lists every connection ID that serves this " +
    "datacenter (a datacenter can have one per availability zone)."

  private def route(connectionId: String, hostId: UUID) =
    Route(connectionId, hostId, address = Some("10.1.0.1"), port = Some(9042), tlsPort = Some(9142))

  private def evaluate(
    connectionIds: Seq[String],
    routes: Seq[Route],
    localHostIds: Set[UUID] = Set(dc1Node1, dc1Node2),
    useTls: Boolean = false,
    addressOverrides: Map[String, String] = Map.empty): Either[String, Seq[String]] =
    ClientRoutesVerifier.evaluate(
      localDC = "dc1",
      connectionIds = connectionIds,
      routes = routes,
      useTls = useTls,
      localHostIds = localHostIds,
      knownDCs = Set("dc1", "dc2"),
      addressOverrides = addressOverrides)

  private def errorOf(result: Either[String, Seq[String]]): String =
    result.fold(identity, warnings => fail(s"Expected an error, got warnings $warnings"))

  "Client routes verification" should "pass when every local node has a route" in {
    evaluate(Seq("conn-dc1"), Seq(route("conn-dc1", dc1Node1), route("conn-dc1", dc1Node2))) shouldBe Right(Nil)
  }

  it should "pass with connection IDs of several datacenters" in {
    evaluate(
      Seq("conn-dc1", "conn-dc2"),
      Seq(route("conn-dc1", dc1Node1), route("conn-dc1", dc1Node2), route("conn-dc2", dc2Node1))
    ) shouldBe Right(Nil)
  }

  it should "pass when the connection IDs of the availability zones together cover the local datacenter" in {
    evaluate(
      Seq("conn-dc1-az1", "conn-dc1-az2", "conn-dc1-az3", "conn-dc2-az1"),
      Seq(
        route("conn-dc1-az1", dc1Node1),
        route("conn-dc1-az2", dc1Node2),
        route("conn-dc1-az3", dc1Node3),
        route("conn-dc2-az1", dc2Node1)),
      localHostIds = Set(dc1Node1, dc1Node2, dc1Node3)
    ) shouldBe Right(Nil)
  }

  it should "fail naming the node of an availability zone whose connection ID is missing" in {
    val error = errorOf(evaluate(
      Seq("conn-dc1-az1", "conn-dc1-az2", "conn-dc2-az1"),
      Seq(route("conn-dc1-az1", dc1Node1), route("conn-dc1-az2", dc1Node2), route("conn-dc2-az1", dc2Node1)),
      localHostIds = Set(dc1Node1, dc1Node2, dc1Node3)))

    error should include("1 of 3 node(s) in the local datacenter 'dc1' have no usable client route")
    error should include(s"(host IDs: $dc1Node3)")
    error should include(IdsHint)
  }

  it should "pass when a node is published under several connection IDs" in {
    evaluate(
      Seq("conn-dc1-az1", "conn-dc1-az2"),
      Seq(route("conn-dc1-az1", dc1Node1), route("conn-dc1-az1", dc1Node2), route("conn-dc1-az2", dc1Node2))
    ) shouldBe Right(Nil)
  }

  it should "count a node whose route lacks tls_port when another connection ID has a usable route for it" in {
    evaluate(
      Seq("conn-dc1-az1", "conn-dc1-az2"),
      Seq(
        route("conn-dc1-az1", dc1Node1),
        route("conn-dc1-az1", dc1Node2).copy(tlsPort = None),
        route("conn-dc1-az2", dc1Node2)),
      useTls = true
    ) shouldBe Right(Nil)
  }

  it should "fail when a local node has no route" in {
    val error = errorOf(evaluate(Seq("conn-dc1"), Seq(route("conn-dc1", dc1Node1))))
    error should include("1 of 2 node(s) in the local datacenter 'dc1' have no usable client route")
    error should include(dc1Node2.toString)
    error shouldNot include(dc1Node1.toString)
    error should include(IdsHint)
    error shouldNot include("tls_port")
  }

  it should "fail when the connection ID matches no route (e.g. a typo)" in {
    val error = errorOf(evaluate(Seq("conn-typo"), Nil))
    error should include("2 of 2 node(s)")
    error should include("conn-typo")
  }

  it should "not count routes of remote datacenters for local nodes" in {
    evaluate(Seq("conn-dc2"), Seq(route("conn-dc2", dc2Node1))) shouldBe a[Left[_, _]]
  }

  it should "not count routes without tls_port when SSL is enabled" in {
    val error = errorOf(evaluate(
      Seq("conn-dc1"),
      Seq(route("conn-dc1", dc1Node1), route("conn-dc1", dc1Node2).copy(tlsPort = None)),
      useTls = true))
    error should include("1 of 2 node(s)")
    error should include(dc1Node2.toString)
    error should include("have no 'tls_port'")
    error should include("TLS is enabled")
    error should include(SSLEnabledParam.name)
    error shouldNot include(IdsHint)
  }

  it should "ignore a missing tls_port but not a missing port when SSL is disabled" in {
    val routes = Seq(route("conn-dc1", dc1Node1), route("conn-dc1", dc1Node2))
    evaluate(Seq("conn-dc1"), routes.map(_.copy(tlsPort = None))) shouldBe Right(Nil)

    val error = errorOf(evaluate(Seq("conn-dc1"), Seq(routes.head, routes(1).copy(port = None))))
    error should include("have no 'port'")
    error should include("TLS is disabled")
  }

  it should "report both a missing route and a route without the needed port" in {
    val error = errorOf(evaluate(Seq("conn-dc1"), Seq(route("conn-dc1", dc1Node1).copy(tlsPort = None)), useTls = true))
    error should include("2 of 2 node(s)")
    error should include(IdsHint)
    error should include("The routes of 1 of them have no 'tls_port'")
  }

  it should "fail when the local datacenter has no nodes, listing the known datacenters" in {
    evaluate(Seq("conn-dc1"), Nil, localHostIds = Set.empty) shouldBe
      Left("Client routes: no nodes found in the local datacenter 'dc1'. Known datacenters: dc1, dc2.")
  }

  private val AddressHint = "have no address, or an empty one: set the address of their connection ID with " +
    s"connectionId=address in ${ClientRoutesEndpointsParam.name}."

  it should "not count a route without address, which the driver skips" in {
    val error = errorOf(evaluate(
      Seq("conn-dc1"),
      Seq(route("conn-dc1", dc1Node1), route("conn-dc1", dc1Node2).copy(address = None))))

    error should include("1 of 2 node(s)")
    error should include(dc1Node2.toString)
    error should include(s"The routes of 1 of them $AddressHint")
    error shouldNot include(IdsHint)
  }

  it should "not count a route with an empty address, which the driver skips" in {
    val error = errorOf(evaluate(
      Seq("conn-dc1"),
      Seq(route("conn-dc1", dc1Node1), route("conn-dc1", dc1Node2).copy(address = Some("")))))

    error should include("1 of 2 node(s)")
    error should include(dc1Node2.toString)
    error should include(s"The routes of 1 of them $AddressHint")
  }

  it should "use the connection's address override in place of an empty or missing published address" in {
    val routes = Seq(
      route("conn-dc1", dc1Node1).copy(address = Some("")),
      route("conn-dc1", dc1Node2).copy(address = None))

    evaluate(Seq("conn-dc1"), routes, addressOverrides = Map("conn-dc1" -> "vpce.internal")) shouldBe Right(Nil)
  }

  it should "not let an unusable route affect the other routes, as the driver skips each one on its own" in {
    val routes = Seq(
      route("conn-dc1", dc1Node1),
      route("conn-dc1", dc1Node2),
      route("conn-dc1-az2", dc1Node2).copy(address = Some("")),
      route("conn-dc2", dc2Node1).copy(address = Some(""), port = Some(70000)))

    evaluate(Seq("conn-dc1", "conn-dc1-az2", "conn-dc2"), routes) shouldBe Right(Nil)
  }

  it should "not count a route whose port is outside 1-65535, only for the port the connection uses" in {
    val routes = Seq(route("conn-dc1", dc1Node1), route("conn-dc1", dc1Node2).copy(tlsPort = Some(70000)))

    evaluate(Seq("conn-dc1"), routes) shouldBe Right(Nil)
    val error = errorOf(evaluate(Seq("conn-dc1"), routes, useTls = true))
    error should include("1 of 2 node(s)")
    error should include(dc1Node2.toString)
    error should include("The routes of 1 of them have a 'tls_port' outside 1-65535.")
  }

  it should "warn about connection IDs without routes when the local datacenter is covered" in {
    evaluate(
      Seq("conn-dc1", "conn-unused"),
      Seq(route("conn-dc1", dc1Node1), route("conn-dc1", dc1Node2))
    ) shouldBe Right(Seq("Client routes: connection ID 'conn-unused' has no entries in system.client_routes."))
  }

  /** A session whose metadata knows the given nodes and whose queries reach no node. */
  private def unreachableSession(nodesByDC: (UUID, String)*): CqlSession =
    sessionFailingWith(new NoNodeAvailableException(), nodesByDC: _*)

  /** A session whose metadata knows the given nodes and whose queries fail with `error`. */
  private def sessionFailingWith(error: Throwable, nodesByDC: (UUID, String)*): CqlSession = {
    val session = mock(classOf[CqlSession])
    val context = mock(classOf[DriverContext])
    when(context.getSslEngineFactory).thenReturn(Optional.empty())
    when(session.getContext).thenReturn(context)
    val metadata = mock(classOf[Metadata])
    val nodes = nodesByDC.map { case (hostId, dc) =>
      val node = mock(classOf[Node])
      when(node.getHostId).thenReturn(hostId)
      when(node.getDatacenter).thenReturn(dc)
      hostId -> node
    }
    when(metadata.getNodes).thenReturn(nodes.toMap.asJava)
    when(session.getMetadata).thenReturn(metadata)
    when(session.execute(any(classOf[Statement[_]]))).thenThrow(error)
    session
  }

  it should "explain that no local node could be reached to read the routes" in {
    val session = unreachableSession(dc1Node1 -> "dc1", dc2Node1 -> "dc2")

    val error = intercept[IllegalStateException](
      ClientRoutesVerifier.verify(session, ClientRoutesConf(Seq(ClientRouteEndpointConf("conn-dc1"))), "dc1"))

    error.getMessage should include("no node of the local datacenter 'dc1' could be reached")
    error.getMessage should include("(configured: conn-dc1)")
    error.getMessage should include("publish an address and a 'port'")
    error.getMessage should include("the role may read system.client_routes (a role that is not a superuser needs " +
      "GRANT SELECT ON system.client_routes TO <the role of spark.cassandra.auth.username>)")
    error.getCause shouldBe a[NoNodeAvailableException]
  }

  it should "explain that the role may not read the routes, which keeps the driver from routing any node" in {
    val denied = new UnauthorizedException(mock(classOf[Node]),
      "User app has no SELECT permission on <table system.client_routes> or any of its parents")
    val session = sessionFailingWith(denied, dc1Node1 -> "dc1")

    val error = intercept[IllegalStateException](
      ClientRoutesVerifier.verify(session, ClientRoutesConf(Seq(ClientRouteEndpointConf("conn-dc1"))), "dc1"))

    error.getMessage should include("Client routes: User app has no SELECT permission on <table system.client_routes>")
    error.getMessage should include("The driver reads the routes with the same role, so it cannot route any node")
    error.getMessage should include(ClientRoutesVerifier.GrantHint)
    ClientRoutesVerifier.GrantHint should include(
      "GRANT SELECT ON system.client_routes TO <the role of spark.cassandra.auth.username>")
    error.getCause shouldBe denied
  }

  it should "report a local datacenter without nodes without querying it" in {
    val session = unreachableSession(dc1Node1 -> "dc1", dc2Node1 -> "dc2")

    val error = intercept[IllegalStateException](
      ClientRoutesVerifier.verify(session, ClientRoutesConf(Seq(ClientRouteEndpointConf("conn-dc1"))), "dc9"))

    error.getMessage shouldBe "Client routes: no nodes found in the local datacenter 'dc9'. Known datacenters: dc1, dc2."
    verify(session, never()).execute(any(classOf[Statement[_]]))
  }

  it should "only run on the Spark driver" in {
    def withSparkEnv[T](executorId: Option[String])(code: => T): T = {
      val original = SparkEnv.get
      SparkEnv.set(executorId.map { id =>
        val env = mock(classOf[SparkEnv])
        when(env.executorId).thenReturn(id)
        env
      }.orNull)
      try code finally SparkEnv.set(original)
    }

    withSparkEnv(None)(ClientRoutesVerifier.runsOnSparkDriver) shouldBe true
    withSparkEnv(Some("driver"))(ClientRoutesVerifier.runsOnSparkDriver) shouldBe true
    withSparkEnv(Some("1"))(ClientRoutesVerifier.runsOnSparkDriver) shouldBe false
  }

  "Client routes verification setting" should "be enabled by default" in {
    val conf = new SparkConf(loadDefaults = false)
      .set(LocalDCParam.name, "dc1")
      .set(ClientRoutesEndpointsParam.name, "conn-dc1")
    CassandraConnectorConf.fromSparkConf(conf).clientRoutes.map(_.verify) shouldBe Some(true)
    CassandraConnectorConf.fromSparkConf(conf.set(ClientRoutesVerifyParam.name, "false"))
      .clientRoutes.map(_.verify) shouldBe Some(false)
  }
}
