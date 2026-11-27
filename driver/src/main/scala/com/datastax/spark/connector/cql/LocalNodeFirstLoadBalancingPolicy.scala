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

import scala.collection.mutable
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

  /** Whether each node of the data center this policy uses, by host ID, is local to this JVM.
    *
    * Recorded when the policy learns about a node (init, onAdd and onUp) rather than while it plans queries: telling a
    * node reached through a client route from one that falls back to its own address takes a route lookup, which
    * resolves the route's address in DNS. Replaced as a whole, so that query plans always read a complete map. A route
    * added or removed later counts from the node's next onAdd or onUp. */
  @volatile private var localities = Map.empty[UUID, Boolean]

  private def distance(node: Node): NodeDistance =
    if (isInDataCenterToUse(node)) {
      sameDCNodeDistance(node)
    } else {
      // this insures we keep remote hosts out of our list entirely, even when we get notified of newly joined nodes
      NodeDistance.IGNORED
    }

  private def isInDataCenterToUse(node: Node): Boolean =
    nodeFilter.forall(_.test(node)) && node.getDatacenter == dcToUse

  /** The data center this policy sends requests to, known once the policy is initialized. */
  def localDataCenter: String = dcToUse

  /** The recorded locality of the nodes, by host ID. */
  private[cql] def localitySnapshot: Map[UUID, Boolean] = localities

  private def clientRoutesMonitor: Option[ClientRoutesTopologyMonitor] =
    context.asInstanceOf[InternalDriverContext].getTopologyMonitor match {
      case monitor: ClientRoutesTopologyMonitor => Some(monitor)
      case _ => None
    }

  /** Looks up whether nodes have a client route, if the session uses client routes. */
  private def routeLookup: Option[UUID => Boolean] = clientRoutesMonitor.map(monitor => hasClientRoute(monitor, _))

  override def init(nodes: util.Map[UUID, Node], distanceReporter: LoadBalancingPolicy.DistanceReporter): Unit = {
    this.nodes = nodes.asScala.values.toSet
    // The data center inference and the locality of the nodes share the route lookups: at most one per node
    val initRouteLookup = routeLookup.map { hasRoute =>
      val lookups = mutable.Map.empty[UUID, Boolean]
      (hostId: UUID) => lookups.getOrElseUpdate(hostId, hasRoute(hostId))
    }
    dcToUse = localDC.getOrElse(inferDataCenter(initRouteLookup))
    this.distanceReporter = distanceReporter

    this.nodes.foreach(updateLocality(_, initRouteLookup))
    this.nodes.foreach { node =>
      distanceReporter.setDistance(node, distance(node))
    }
  }

  /** Records whether the node is local, if it is in the data center this policy uses, and forgets it otherwise.
    *
    * @throws IllegalStateException if the client routes topology monitor is closed */
  private def updateLocality(node: Node, hasRoute: Option[UUID => Boolean]): Unit =
    Option(node.getHostId).foreach { hostId =>
      if (isInDataCenterToUse(node)) {
        val local = node.getEndPoint match {
          case endPoint: ClientRoutesEndPoint => hasRoute.exists(routed => !routed(hostId) && isLocalFallback(endPoint))
          case _ => isLocalHost(node)
        }
        synchronized { localities = localities.updated(hostId, local) }
      } else {
        forgetLocality(node)
      }
    }

  private def forgetLocality(node: Node): Unit =
    Option(node.getHostId).foreach(hostId => synchronized { localities -= hostId })

  /** Whether the node is local, as recorded. A node not recorded yet is checked by its address, unless it is reached
    * through a client route: query plans never look a route up. */
  private def isLocal(node: Node): Boolean =
    Option(node.getHostId).flatMap(localities.get).getOrElse {
      node.getEndPoint match {
        case _: ClientRoutesEndPoint => false
        case _ => isLocalHost(node)
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
  private def inferDataCenter(hasRoute: Option[UUID => Boolean]): String = {
    val routedDCs = hasRoute.map(routedDataCenters(nodes, _)).getOrElse(Set.empty[String])
    if (routedDCs.size > 1)
      throw new IllegalArgumentException(s"Client routes cover datacenters ${routedDCs.toSeq.sorted.mkString(", ")}; " +
        s"set spark.cassandra.connection.localDC")
    routedDCs.headOption.getOrElse {
      val contactPoints = metadataManager.getContactPoints.asScala.flatMap(toAddress).toSet
      if (hasRoute.isDefined && contactPointDataCenters(contactPoints, nodes).isEmpty)
        throw new IllegalArgumentException(
          "Cannot determine the local datacenter: no node has a usable client route for the configured connection " +
            "IDs, and no node matches a contact point address. Check spark.cassandra.connection.clientRoutes.endpoints " +
            "and that the routes publish the port the connection needs (tls_port when SSL is enabled), or set " +
            "spark.cassandra.connection.localDC to get a report of the nodes without a usable route.")
      determineDataCenter(contactPoints, nodes)
    }
  }

  private def tokenUnawareQueryPlan(statement: Request): Seq[Node] = {
    sortByStatusAndProximity(dcToUse, nodes, isLocal)
  }

  private def replicaAwareQueryPlan(statement: Request, replicas: Set[Node]): Seq[Node] = {
    val (localReplica, otherReplicas) = replicas.partition(isLocal)
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
    updateLocality(node, routeLookup)
    distanceReporter.setDistance(node, distance(node))
  }

  override def onRemove(node: Node): Unit = {
    nodes -= node
    forgetLocality(node)
  }

  override def close(): Unit = synchronized {
    localities = Map.empty
  }

  override def onUp(node: Node): Unit = {
    updateLocality(node, routeLookup)
    distanceReporter.setDistance(node, distance(node))
  }

  override def onDown(node: Node): Unit = {}

  private def sameDCNodeDistance(node: Node): NodeDistance =
    if (isLocal(node))
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
    * Returns false for a node with a [[ClientRoutesEndPoint]] without looking its route up, which can take a DNS
    * lookup. The policy itself records which of those nodes have no route and fall back to an address of this host. */
  def isLocalHost(node: Node): Boolean = node.getEndPoint match {
    case _: ClientRoutesEndPoint => false
    case _ => toAddress(node).exists(isLocalAddress)
  }

  private def isLocalAddress(address: InetSocketAddress): Boolean =
    Option(address.getAddress).exists(host => host.isLoopbackAddress || localAddresses.contains(host))

  /** Whether a node with a [[ClientRoutesEndPoint]] and without a client route falls back to an address of this host.
    *
    * Only for a node without a route: a node with one is never local, even when the route's address is, since the
    * private endpoint forwards the connection to the node. */
  private def isLocalFallback(endPoint: ClientRoutesEndPoint): Boolean = endPoint.resolve() match {
    case address: InetSocketAddress => isLocalAddress(address)
    case _ => false
  }

  /** Whether the node has a client route. A route whose address does not resolve counts.
    *
    * Relies on the driver's internal [[ClientRoutesTopologyMonitor.resolve]], which returns null without a DNS lookup
    * for a node without a route, and resolves the route's address in DNS otherwise.
    *
    * @throws IllegalStateException if the monitor is closed, which `resolve` reports before looking for a route */
  private[cql] def hasClientRoute(monitor: ClientRoutesTopologyMonitor, hostId: UUID): Boolean =
    try monitor.resolve(hostId) != null
    catch {
      // Thrown once the route is found, when its address does not resolve
      case _: UnknownHostException => true
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
  def clientRoutesDataCenters(monitor: ClientRoutesTopologyMonitor, allNodes: Set[Node]): Set[String] =
    routedDataCenters(allNodes, hasClientRoute(monitor, _))

  private def routedDataCenters(allNodes: Set[Node], hasRoute: UUID => Boolean): Set[String] =
    allNodes
      .groupBy(node => Option(node.getDatacenter))
      .collect { case (Some(dc), dcNodes) if dcNodes.exists(node => Option(node.getHostId).exists(hasRoute)) => dc }
      .toSet

  /** Sorts nodes in the following order:
    * 1. local node in a given DC
    * 2. live nodes in a given DC
    * 3. the rest of nodes in a given DC
    *
    * Nodes within a group are ordered randomly. Nodes from other DCs are not included. */
  def sortNodesByStatusAndProximity(dc: String, nodesToSort: Set[Node]): Seq[Node] =
    sortByStatusAndProximity(dc, nodesToSort, isLocalHost)

  /** [[sortNodesByStatusAndProximity]], with the given test of whether a node is local. */
  private def sortByStatusAndProximity(dc: String, nodesToSort: Set[Node], isLocal: Node => Boolean): Seq[Node] = {
    val grouped = nodesToSort.groupBy {
      case node if node.getDatacenter != dc => None
      case node if node.getState != NodeState.UP => Some(2)
      case node if !isLocal(node) => Some(1)
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
