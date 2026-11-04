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

import java.net.{InetSocketAddress, UnknownHostException}
import java.nio.ByteBuffer
import java.util.{Optional, UUID}

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER
import com.datastax.oss.driver.api.core.config.{DriverConfig, DriverExecutionProfile}
import com.datastax.oss.driver.api.core.loadbalancing.{LoadBalancingPolicy, NodeDistance}
import com.datastax.oss.driver.api.core.metadata.{EndPoint, Metadata, Node, NodeState, TokenMap}
import com.datastax.oss.driver.api.core.session.{Request, Session}
import com.datastax.oss.driver.internal.core.context.InternalDriverContext
import com.datastax.oss.driver.internal.core.metadata.{ClientRoutesEndPoint, ClientRoutesTopologyMonitor, DefaultNode, MetadataManager, TopologyMonitor}
import org.mockito.Mockito._
import org.mockito.{ArgumentMatchers => m}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.jdk.CollectionConverters._

/** Local DC inference and locality of nodes reached through client routes (ScyllaDB Cloud private endpoints).
  *
  * These tests pin the internal driver API the policy relies on: [[ClientRoutesTopologyMonitor.resolve]] and
  * [[ClientRoutesEndPoint]]. */
class LocalNodeFirstLoadBalancingPolicyClientRoutesSpec extends FlatSpec with Matchers with MockitoSugar {

  private val profileName = "someProfile"

  private class Setup(localDC: Option[String] = None, clientRoutes: Boolean = true) {
    val context: InternalDriverContext = mock[InternalDriverContext]
    val monitor: ClientRoutesTopologyMonitor = mock[ClientRoutesTopologyMonitor]
    val metadataManager: MetadataManager = mock[MetadataManager]
    val reporter: LoadBalancingPolicy.DistanceReporter = mock[LoadBalancingPolicy.DistanceReporter]

    private val config = mock[DriverConfig]
    private val profile = mock[DriverExecutionProfile]
    when(profile.getString(m.eq(LOAD_BALANCING_LOCAL_DATACENTER), m.any())).thenReturn(localDC.orNull)
    when(config.getProfile(m.eq(profileName))).thenReturn(profile)
    when(context.getConfig).thenReturn(config)
    when(context.getMetadataManager).thenReturn(metadataManager)
    when(context.getTopologyMonitor).thenReturn(if (clientRoutes) monitor else mock[TopologyMonitor])
    when(metadataManager.getContactPoints).thenReturn(Set.empty[DefaultNode].asJava)

    /** A node whose end point resolves to its route, or to `fallback` (its own address) without a route. */
    def node(dc: String, route: Option[InetSocketAddress], fallback: String = "10.0.0.1"): Node = {
      val hostId = UUID.randomUUID()
      val endPoint = mock[ClientRoutesEndPoint]
      when(endPoint.resolve()).thenReturn(route.getOrElse(new InetSocketAddress(fallback, 9042)))
      when(monitor.resolve(hostId)).thenReturn(route.orNull)
      nodeWith(hostId, dc, endPoint)
    }

    /** Contact points are separate nodes without host ID nor DC. */
    def contactPoints(addresses: String*): Unit = {
      val nodes = addresses.map { address =>
        val endPoint = mock[EndPoint]
        when(endPoint.resolve()).thenReturn(new InetSocketAddress(address, 9042))
        val node = mock[DefaultNode]
        when(node.getEndPoint).thenReturn(endPoint)
        node
      }
      when(metadataManager.getContactPoints).thenReturn(nodes.toSet.asJava)
    }

    def init(nodes: Node*): LocalNodeFirstLoadBalancingPolicy = {
      val policy = new LocalNodeFirstLoadBalancingPolicy(context, profileName)
      policy.init(nodes.map(node => node.getHostId -> node).toMap.asJava, reporter)
      policy
    }
  }

  private def nodeWith(hostId: UUID, dc: String, endPoint: EndPoint): Node = {
    val node = mock[Node]
    when(node.getHostId).thenReturn(hostId)
    when(node.getDatacenter).thenReturn(dc)
    when(node.getEndPoint).thenReturn(endPoint)
    when(node.getState).thenReturn(NodeState.UP)
    node
  }

  /** A node whose end point is not a client route end point. */
  private def plainNode(dc: String, address: String): Node = {
    val endPoint = mock[EndPoint]
    when(endPoint.resolve()).thenReturn(new InetSocketAddress(address, 9042))
    nodeWith(UUID.randomUUID(), dc, endPoint)
  }

  private val keyspace = CqlIdentifier.fromInternal("ks")
  private val routingKey = ByteBuffer.wrap(Array[Byte](1, 2, 3))

  /** A request whose partition key the given nodes own. */
  private def requestTo(setup: Setup, replicas: Node*): Request = {
    val tokenMap = mock[TokenMap]
    when(tokenMap.getReplicas(keyspace, routingKey)).thenReturn(replicas.toSet.asJava)
    val metadata = mock[Metadata]
    when(metadata.getTokenMap).thenReturn(Optional.of(tokenMap))
    when(setup.metadataManager.getMetadata).thenReturn(metadata)
    val request = mock[Request]
    when(request.getKeyspace).thenReturn(keyspace)
    when(request.getRoutingKey).thenReturn(routingKey)
    request
  }

  private def plan(policy: LocalNodeFirstLoadBalancingPolicy, request: Request = null): Seq[Node] =
    policy.newQueryPlan(request, if (request == null) null else mock[Session]).asScala.toSeq

  private def route(port: Int) = Some(new InetSocketAddress("10.1.0.1", port))

  /** The route published by the private endpoint of an availability zone (one connection ID per zone). */
  private def zoneRoute(zone: Int) = Some(new InetSocketAddress(s"10.1.$zone.1", 9042))

  "LocalNodeFirstLoadBalancingPolicy with client routes" should "use the DC of the nodes that have a client route" in
    new Setup {
      val dc1Node1 = node("dc1", route(9001))
      val dc1Node2 = node("dc1", route(9002))
      val dc2Node = node("dc2", None)

      init(dc1Node1, dc1Node2, dc2Node).localDataCenter shouldBe "dc1"

      verify(reporter).setDistance(dc1Node1, NodeDistance.REMOTE)
      verify(reporter).setDistance(dc1Node2, NodeDistance.REMOTE)
      verify(reporter).setDistance(dc2Node, NodeDistance.IGNORED)
    }

  it should "fail when the client routes cover several DCs" in new Setup {
    val error = intercept[IllegalArgumentException](init(node("dc2", route(9002)), node("dc1", route(9001))))
    error.getMessage should include("Client routes cover datacenters dc1, dc2")
    error.getMessage should include("spark.cassandra.connection.localDC")
  }

  it should "use the DC whose nodes are routed by the connection IDs of its availability zones" in new Setup {
    val zoneNodes = (1 to 3).map(zone => node("dc1", zoneRoute(zone)))
    val dc2Nodes = (1 to 3).map(_ => node("dc2", None))

    init(zoneNodes ++ dc2Nodes: _*).localDataCenter shouldBe "dc1"

    zoneNodes.foreach(n => verify(reporter).setDistance(n, NodeDistance.REMOTE))
    dc2Nodes.foreach(n => verify(reporter).setDistance(n, NodeDistance.IGNORED))
  }

  it should "fail when the connection IDs of several availability zones span two DCs" in new Setup {
    val nodes = (1 to 3).map(zone => node("dc1", zoneRoute(zone))) ++ (4 to 5).map(zone => node("dc2", zoneRoute(zone)))

    intercept[IllegalArgumentException](init(nodes: _*)).getMessage should
      include("Client routes cover datacenters dc1, dc2")
  }

  it should "count a route whose address does not resolve" in new Setup {
    val unresolved = node("dc2", None)
    when(monitor.resolve(unresolved.getHostId)).thenThrow(new UnknownHostException("vpce.invalid"))

    init(node("dc1", None), unresolved).localDataCenter shouldBe "dc2"
  }

  it should "not take a closed topology monitor for a route" in new Setup {
    val dc1Node = node("dc1", None)
    val dc2Node = node("dc2", None)
    // resolve reports a closed monitor before it looks for a route
    Seq(dc1Node, dc2Node).foreach(n =>
      when(monitor.resolve(n.getHostId)).thenThrow(new IllegalStateException("Topology monitor is closed")))

    intercept[IllegalStateException](init(dc1Node, dc2Node)).getMessage shouldBe "Topology monitor is closed"
  }

  it should "look the route of each node up at most once when it infers the DC and records locality" in new Setup {
    val dc1Nodes = (1 to 3).map(zone => node("dc1", zoneRoute(zone)))
    val dc2Nodes = (1 to 2).map(_ => node("dc2", None))

    init(dc1Nodes ++ dc2Nodes: _*).localDataCenter shouldBe "dc1"

    // Resolving a route looks its address up in DNS
    dc1Nodes.foreach(n => verify(monitor, times(1)).resolve(n.getHostId))
    dc2Nodes.foreach(n => verify(monitor, org.mockito.Mockito.atMost(1)).resolve(n.getHostId))
  }

  it should "fail with a client routes message when neither routes nor contact points identify the DC" in new Setup {
    contactPoints("10.9.9.9")

    val error = intercept[IllegalArgumentException](
      init(node("dc1", None, fallback = "10.0.0.1"), node("dc2", None, fallback = "10.0.0.2")))

    error.getMessage should include("no node has a usable client route")
    error.getMessage should include("spark.cassandra.connection.clientRoutes.endpoints")
    error.getMessage should include("spark.cassandra.connection.localDC")
  }

  it should "prefer the configured local DC" in new Setup(localDC = Some("dc2")) {
    val dc1Node = node("dc1", route(9001))
    val dc2Node = node("dc2", route(9002))

    init(dc1Node, dc2Node).localDataCenter shouldBe "dc2"

    // Only the routes of the local DC's nodes are looked up, to record whether those nodes are local
    verify(monitor, never()).resolve(dc1Node.getHostId)
    verify(monitor, times(1)).resolve(dc2Node.getHostId)
    verify(reporter).setDistance(dc1Node, NodeDistance.IGNORED)
    verify(reporter).setDistance(dc2Node, NodeDistance.REMOTE)
  }

  it should "match contact points by address when no node has a route" in new Setup {
    contactPoints("10.0.0.2")

    init(node("dc1", None, fallback = "10.0.0.1"), node("dc2", None, fallback = "10.0.0.2")).localDataCenter shouldBe "dc2"
  }

  it should "never treat a node with a route as local, even when the route's address is a loopback address" in
    new Setup(localDC = Some("dc1")) {
      val loopbackRoute = node("dc1", Some(new InetSocketAddress("127.0.0.1", 19042)))
      val remote = plainNode("dc1", "10.0.0.9")

      val policy = init(loopbackRoute, remote)

      LocalNodeFirstLoadBalancingPolicy.isLocalHost(loopbackRoute) shouldBe false
      verify(reporter).setDistance(loopbackRoute, NodeDistance.REMOTE)
      policy.localitySnapshot shouldBe Map(loopbackRoute.getHostId -> false, remote.getHostId -> false)
      // A routed node is never checked by the address its end point resolves to
      verify(loopbackRoute.getEndPoint, never()).resolve()
    }

  it should "treat a node without a route as local when it falls back to an address of this host" in
    new Setup(localDC = Some("dc1")) {
      val localFallback = node("dc1", None, fallback = "127.0.0.1")
      val remoteFallback = node("dc1", None, fallback = "10.0.0.1")
      val routed = node("dc1", Some(new InetSocketAddress("127.0.0.2", 19042)))

      val policy = init(localFallback, remoteFallback, routed)

      verify(reporter).setDistance(localFallback, NodeDistance.LOCAL)
      verify(reporter).setDistance(remoteFallback, NodeDistance.REMOTE)
      verify(reporter).setDistance(routed, NodeDistance.REMOTE)
      plan(policy).head shouldBe localFallback
      plan(policy, requestTo(this, routed, remoteFallback, localFallback)).head shouldBe localFallback
      plan(policy, requestTo(this, routed, remoteFallback)).take(2).toSet shouldBe Set(routed, remoteFallback)
    }

  it should "treat a node whose route address does not resolve as routed, so not local" in
    new Setup(localDC = Some("dc1")) {
      val unresolved = node("dc1", None, fallback = "127.0.0.1")
      when(monitor.resolve(unresolved.getHostId)).thenThrow(new UnknownHostException("vpce.invalid"))

      init(unresolved).localitySnapshot shouldBe Map(unresolved.getHostId -> false)
      verify(reporter).setDistance(unresolved, NodeDistance.REMOTE)
    }

  it should "plan queries from the recorded locality, without looking routes up" in new Setup(localDC = Some("dc1")) {
    val localFallback = node("dc1", None, fallback = "127.0.0.1")
    val routed = node("dc1", route(9001))
    val policy = init(localFallback, routed)
    clearInvocations(monitor, localFallback.getEndPoint, routed.getEndPoint)

    plan(policy) should contain theSameElementsAs Seq(localFallback, routed)
    plan(policy, requestTo(this, localFallback, routed)).head shouldBe localFallback

    verify(monitor, never()).resolve(m.any())
    verify(localFallback.getEndPoint, never()).resolve()
    verify(routed.getEndPoint, never()).resolve()
  }

  it should "update the recorded locality when a node is added, comes up or is removed, and clear it on close" in
    new Setup(localDC = Some("dc1")) {
      val first = node("dc1", route(9001))
      val policy = init(first)

      val added = node("dc1", None, fallback = "127.0.0.1")
      policy.onAdd(added)
      verify(reporter).setDistance(added, NodeDistance.LOCAL)
      val otherDC = node("dc2", None, fallback = "127.0.0.1")
      policy.onAdd(otherDC)
      verify(reporter).setDistance(otherDC, NodeDistance.IGNORED)
      policy.localitySnapshot shouldBe Map(first.getHostId -> false, added.getHostId -> true)

      // The route of the first node is removed, and the node falls back to its own address, here a local one
      when(monitor.resolve(first.getHostId)).thenReturn(null)
      when(first.getEndPoint.resolve()).thenReturn(new InetSocketAddress("127.0.0.1", 9042))
      policy.localitySnapshot(first.getHostId) shouldBe false
      policy.onUp(first)
      policy.localitySnapshot(first.getHostId) shouldBe true
      verify(reporter).setDistance(first, NodeDistance.LOCAL)

      policy.onRemove(added)
      policy.localitySnapshot shouldBe Map(first.getHostId -> true)

      policy.close()
      policy.localitySnapshot shouldBe empty
    }

  it should "let a closed topology monitor or an unexpected error of a route lookup through" in
    new Setup(localDC = Some("dc1")) {
      val policy = init(node("dc1", route(9001)))

      val added = node("dc1", None)
      when(monitor.resolve(added.getHostId)).thenThrow(new IllegalStateException("Topology monitor is closed"))
      intercept[IllegalStateException](policy.onAdd(added)).getMessage shouldBe "Topology monitor is closed"

      val upAgain = node("dc1", None)
      when(monitor.resolve(upAgain.getHostId)).thenThrow(new RuntimeException("unexpected"))
      intercept[RuntimeException](policy.onUp(upAgain)).getMessage shouldBe "unexpected"
    }

  it should "check the nodes without a client route end point by their address" in new Setup(localDC = Some("dc1")) {
    val local = plainNode("dc1", "127.0.0.1")
    val remote = plainNode("dc1", "10.0.0.1")

    init(local, remote).localitySnapshot shouldBe Map(local.getHostId -> true, remote.getHostId -> false)

    verify(reporter).setDistance(local, NodeDistance.LOCAL)
    verify(reporter).setDistance(remote, NodeDistance.REMOTE)
    verify(monitor, never()).resolve(m.any())
  }

  "LocalNodeFirstLoadBalancingPolicy without client routes" should "match contact points by address" in
    new Setup(clientRoutes = false) {
      contactPoints("10.0.0.2")

      init(node("dc1", None, fallback = "10.0.0.1"), node("dc2", None, fallback = "10.0.0.2")).localDataCenter shouldBe "dc2"
      verify(monitor, never()).resolve(m.any())
    }
}
