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
import java.util.UUID

import com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER
import com.datastax.oss.driver.api.core.config.{DriverConfig, DriverExecutionProfile}
import com.datastax.oss.driver.api.core.loadbalancing.{LoadBalancingPolicy, NodeDistance}
import com.datastax.oss.driver.api.core.metadata.{EndPoint, Node}
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
    node
  }

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

  it should "resolve the route of at most one node per DC, as resolving looks the address up in DNS" in new Setup {
    val dc1Nodes = (1 to 3).map(zone => node("dc1", zoneRoute(zone)))
    val dc2Nodes = (1 to 2).map(_ => node("dc2", None))
    val dc1HostIds = dc1Nodes.map(_.getHostId).toSet

    init(dc1Nodes ++ dc2Nodes: _*).localDataCenter shouldBe "dc1"

    verify(monitor, times(1)).resolve(m.argThat[UUID](hostId => dc1HostIds.contains(hostId)))
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

    verify(monitor, never()).resolve(m.any())
    verify(reporter).setDistance(dc1Node, NodeDistance.IGNORED)
    verify(reporter).setDistance(dc2Node, NodeDistance.REMOTE)
  }

  it should "match contact points by address when no node has a route" in new Setup {
    contactPoints("10.0.0.2")

    init(node("dc1", None, fallback = "10.0.0.1"), node("dc2", None, fallback = "10.0.0.2")).localDataCenter shouldBe "dc2"
  }

  it should "never treat a node behind a client route as local, without resolving its route" in new Setup {
    val loopbackRoute = node("dc1", Some(new InetSocketAddress("127.0.0.1", 19042)))

    init(loopbackRoute)

    LocalNodeFirstLoadBalancingPolicy.isLocalHost(loopbackRoute) shouldBe false
    verify(reporter).setDistance(loopbackRoute, NodeDistance.REMOTE)
    verify(loopbackRoute.getEndPoint, never()).resolve()
  }

  "LocalNodeFirstLoadBalancingPolicy without client routes" should "match contact points by address" in
    new Setup(clientRoutes = false) {
      contactPoints("10.0.0.2")

      init(node("dc1", None, fallback = "10.0.0.1"), node("dc2", None, fallback = "10.0.0.2")).localDataCenter shouldBe "dc2"
      verify(monitor, never()).resolve(m.any())
    }
}
