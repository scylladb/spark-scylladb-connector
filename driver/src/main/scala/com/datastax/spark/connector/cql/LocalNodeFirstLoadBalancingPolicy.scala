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

import java.net.{InetSocketAddress, NetworkInterface, UnknownHostException}
import java.nio.ByteBuffer
import java.util
import java.util.UUID
import java.util.function.Predicate

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverOption}
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.loadbalancing.{LoadBalancingPolicy, NodeDistance}
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.oss.driver.api.core.metadata.{Node, NodeState}
import com.datastax.oss.driver.api.core.session.{Request, Session}
import com.datastax.oss.driver.internal.core.context.InternalDriverContext
import com.datastax.oss.driver.internal.core.metadata.{ClientRoutesEndPoint, ClientRoutesTopologyMonitor, MetadataManager}
import com.datastax.oss.driver.internal.core.util.Reflection
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan
import com.datastax.spark.connector.cql.LocalNodeFirstLoadBalancingPolicy.{LoadBalancingShuffleNodes, _}
import com.datastax.spark.connector.util.DriverUtil.{toAddress, toOption}

import scala.jdk.CollectionConverters._
import scala.util.Random

/** Selects local node first and then nodes in local DC in random order. Never selects nodes from other DCs.
  * For writes, if a statement has a routing key set, this LBP is token aware - it prefers the nodes which
  * are replicas of the computed token to the other nodes. */
class LocalNodeFirstLoadBalancingPolicy(context: DriverContext, profileName: String)
  extends LoadBalancingPolicy {

  private val profile = context.getConfig.getProfile(profileName)
  private val localDC: Option[String] = Option(profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null))
  private val shuffleReplicas: Boolean = profile.getBoolean(LoadBalancingShuffleNodes, true)

  private var nodes = Set.empty[Node]
  private var dcToUse = ""
  private val random = new Random
  private val nodeFilter: Option[Predicate[Node]] = {
    val internalContext = context.asInstanceOf[InternalDriverContext]
    Option(internalContext.getNodeFilter(profileName))
      .orElse {
        toOption(Reflection.buildFromConfig(internalContext, profileName, DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS, classOf[Predicate[Node]]))
      }
  }

  private var distanceReporter: LoadBalancingPolicy.DistanceReporter = _
  private val metadataManager: MetadataManager = context.asInstanceOf[InternalDriverContext].getMetadataManager

  private def distance(node: Node): NodeDistance =
    if (nodeFilter.forall(_.test(node)) && node.getDatacenter == dcToUse) {
      sameDCNodeDistance(node)
    } else {
      // this insures we keep remote hosts out of our list entirely, even when we get notified of newly joined nodes
      NodeDistance.IGNORED
    }

  /** The data center this policy sends requests to, known once the policy is initialized. */
  def localDataCenter: String = dcToUse

  override def init(nodes: util.Map[UUID, Node], distanceReporter: LoadBalancingPolicy.DistanceReporter): Unit = {
    this.nodes = nodes.asScala.values.toSet
    dcToUse = localDC.getOrElse(inferDataCenter())
    this.distanceReporter = distanceReporter

    this.nodes.foreach { node =>
      distanceReporter.setDistance(node, distance(node))
    }
  }

  /** Without an explicitly set DC, uses the DC of the nodes reached through client routes, if any. Otherwise checks
    * whether all contact points have the same DC, and if so uses that DC; if not, throws an error.
    *
    * Client routes rewrite every node address, so contact points (private endpoint addresses) cannot be matched with
    * nodes by address. Contact points are not matched by host ID either: they are separate nodes without one.
    *
    * When no node has a usable route, the control node keeps the contact point it was reached through as its address,
    * so the address match usually still finds the local DC, and the client routes verification then reports the
    * missing routes. Only if that match fails too does this fail, with a client routes specific message. */
  private def inferDataCenter(): String = {
    val clientRoutesMonitor = context.asInstanceOf[InternalDriverContext].getTopologyMonitor match {
      case monitor: ClientRoutesTopologyMonitor => Some(monitor)
      case _ => None
    }
    val routedDCs = clientRoutesMonitor.map(clientRoutesDataCenters(_, nodes)).getOrElse(Set.empty[String])
    if (routedDCs.size > 1)
      throw new IllegalArgumentException(s"Client routes cover datacenters ${routedDCs.toSeq.sorted.mkString(", ")}; " +
        s"set spark.cassandra.connection.localDC")
    routedDCs.headOption.getOrElse {
      val contactPoints = metadataManager.getContactPoints.asScala.flatMap(toAddress).toSet
      if (clientRoutesMonitor.isDefined && contactPointDataCenters(contactPoints, nodes).isEmpty)
        throw new IllegalArgumentException(
          "Cannot determine the local datacenter: no node has a usable client route for the configured connection " +
            "IDs, and no node matches a contact point address. Check spark.cassandra.connection.clientRoutes.endpoints " +
            "and that the routes publish the port the connection needs (tls_port when SSL is enabled), or set " +
            "spark.cassandra.connection.localDC to get a report of the nodes without a usable route.")
      determineDataCenter(contactPoints, nodes)
    }
  }

  private def tokenUnawareQueryPlan(statement: Request): Seq[Node] = {
    sortNodesByStatusAndProximity(dcToUse, nodes)
  }

  private def replicaAwareQueryPlan(statement: Request, replicas: Set[Node]): Seq[Node] = {
    val (localReplica, otherReplicas) = replicas.partition(isLocalHost)
    lazy val maybeShuffledOtherReplicas = if (shuffleReplicas) random.shuffle(otherReplicas.toIndexedSeq) else otherReplicas

    lazy val otherNodes = tokenUnawareQueryPlan(statement).toIterator
      .filter(node => !replicas.contains(node) && distance(node) != NodeDistance.IGNORED)

    (localReplica.iterator #:: maybeShuffledOtherReplicas.iterator #:: otherNodes #:: Stream.empty).flatten
  }

  def tokenMap =
    Option(metadataManager.getMetadata.getTokenMap.orElse(null))
      .orElse(throw new IllegalArgumentException("Unable to get Token Metadata"))

  // copied and adjusted from DefaultLoadBalancingPolicy
  private def getReplicas(request: Request, session: Session): Set[Node] = {
    if (request == null || session == null) {
      Set()
    } else {
      Option(request.getKeyspace)
        .orElse(Option(request.getRoutingKeyspace))
        .orElse(toOption(session.getKeyspace))
        .flatMap { keyspace =>

          def replicasForToken(token: Token) = {
            tokenMap.map(_.getReplicas(keyspace, token))
          }

          def replicasForRoutingKey(key: ByteBuffer) = {
            tokenMap.map(_.getReplicas(keyspace, key))
          }

          Option(request.getRoutingToken).flatMap(replicasForToken)
            .orElse(Option(request.getRoutingKey).flatMap(replicasForRoutingKey))
            .map(_.asScala.toSet)
        }.getOrElse(Set())
    }
  }

  override def newQueryPlan(request: Request, session: Session): util.Queue[Node] = {
    val replicas = getReplicas(request, session)
      .filter(node => node.getState == NodeState.UP && distance(node) != NodeDistance.IGNORED)

    val nodes = if (replicas.nonEmpty) {
      replicaAwareQueryPlan(request, replicas)
    } else {
      tokenUnawareQueryPlan(request)
    }
    new SimpleQueryPlan(nodes: _*)
  }

  override def onAdd(node: Node): Unit = {
    // The added host might be a "better" version of a host already in the set.
    // The nodes added in the init call don't have DC and rack set.
    // Therefore we want to really replace the object now, to get full information on DC:
    nodes -= node
    nodes += node
    distanceReporter.setDistance(node, distance(node))
  }

  override def onRemove(node: Node): Unit = {
    nodes -= node
  }

  override def close(): Unit = {}

  override def onUp(node: Node): Unit = {
    distanceReporter.setDistance(node, distance(node))
  }

  override def onDown(node: Node): Unit = {}

  private def sameDCNodeDistance(node: Node): NodeDistance =
    if (isLocalHost(node))
      NodeDistance.LOCAL
    else
      NodeDistance.REMOTE
}

object LocalNodeFirstLoadBalancingPolicy {

  private val random = new Random

  private val localAddresses =
    NetworkInterface.getNetworkInterfaces.asScala.flatMap(_.getInetAddresses.asScala).toSet

  /** Returns true if given host is local host.
    *
    * A node reached through a client route (a private endpoint) is never local: no executor runs on the database nodes
    * behind it. Checking it would also resolve the route address for every replica of every query plan. */
  def isLocalHost(node: Node): Boolean = node.getEndPoint match {
    case _: ClientRoutesEndPoint => false
    case _ =>
      toAddress(node).exists(hostAddress => hostAddress.getAddress.isLoopbackAddress || localAddresses.contains(hostAddress.getAddress))
  }

  /** Returns the data centers of the nodes that have a client route.
    *
    * Relies on the driver's internal [[ClientRoutesTopologyMonitor]], which resolves the routes of the configured
    * connection IDs. A connection ID belongs to one availability zone of one data center, so the connection IDs of a
    * data center (one per availability zone) together identify that data center.
    *
    * `resolve` returns null without a DNS lookup for a node without a route, but looks the route's address up in DNS
    * otherwise, so the check stops at the first routed node of each data center: at most one lookup per data center,
    * which the driver performs anyway when it connects to that node.
    *
    * @throws IllegalStateException if the monitor is closed, which `resolve` reports before looking for a route */
  def clientRoutesDataCenters(monitor: ClientRoutesTopologyMonitor, allNodes: Set[Node]): Set[String] = {
    def hasRoute(hostId: UUID): Boolean =
      try monitor.resolve(hostId) != null
      catch {
        // Thrown once the route is found, when its address does not resolve
        case _: UnknownHostException => true
      }

    allNodes
      .groupBy(node => Option(node.getDatacenter))
      .collect { case (Some(dc), dcNodes) if dcNodes.exists(node => Option(node.getHostId).exists(hasRoute)) => dc }
      .toSet
  }

  /** Sorts nodes in the following order:
    * 1. local node in a given DC
    * 2. live nodes in a given DC
    * 3. the rest of nodes in a given DC
    *
    * Nodes within a group are ordered randomly. Nodes from other DCs are not included. */
  def sortNodesByStatusAndProximity(dc: String, nodesToSort: Set[Node]): Seq[Node] = {
    val grouped = nodesToSort.groupBy {
      case node if node.getDatacenter != dc => None
      case node if node.getState != NodeState.UP => Some(2)
      case node if !isLocalHost(node) => Some(1)
      case _ => Some(0)
    } - None

    grouped.toSeq.sortBy(_._1.get).flatMap {
      case (_, nodes) => random.shuffle(nodes.toIndexedSeq)
    }
  }

  /** Returns a common data center name of the given contact points.
    *
    * For each contact point there must be a [[Node]] in `allNodes` collection in order to determine its data center
    * name. If contact points belong to more than a single data center, an [[IllegalArgumentException]] is thrown.
    */
  def determineDataCenter(contactPoints: Set[InetSocketAddress], allNodes: Set[Node]): String = {
    val dcs = contactPointDataCenters(contactPoints, allNodes)
    assert(dcs.nonEmpty, "There are no contact points in the given set of hosts")
    require(dcs.size == 1, s"Contact points contain multiple data centers: ${dcs.mkString(", ")}")
    dcs.head
  }

  /** Returns the data centers of the nodes whose address is one of the given contact points. */
  private def contactPointDataCenters(contactPoints: Set[InetSocketAddress], allNodes: Set[Node]): Set[String] = {
    val maybeResolved = contactPoints.map(x =>
      if (x.isUnresolved) new InetSocketAddress(x.getHostString, x.getPort) else x)
    allNodes
      .filter(node => toAddress(node).exists(maybeResolved.contains))
      .flatMap(node => Option(node.getDatacenter))
  }

  val LoadBalancingShuffleNodes: DriverOption = new DriverOption {
    override def getPath: String = "basic.load-balancing-policy.shuffle-replicas"
  }

}
