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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Optional, UUID}

import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.`type`.{DataTypes, UserDefinedType}
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.cql.{ColumnDefinition, PreparedStatement, ResultSet, Row}
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance
import com.datastax.oss.driver.api.core.metadata.schema._
import com.datastax.oss.driver.api.core.metadata.token.{Partitioner, Token, TokenRange}
import com.datastax.oss.driver.api.core.metadata.{EndPoint, Metadata, Node, NodeState, TokenMap}
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession, ProtocolVersion, RequestRoutingType}
import com.datastax.oss.driver.internal.core.cql.{DefaultColumnDefinition, DefaultColumnDefinitions, DefaultPreparedStatement}
import com.datastax.oss.driver.internal.core.metadata.schema.{DefaultColumnMetadata, DefaultKeyspaceMetadata, DefaultTableMetadata}
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token
import com.datastax.oss.protocol.internal.ProtocolConstants
import com.datastax.oss.protocol.internal.response.result.{ColumnSpec, RawType}
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._

/** A cluster that mocked sessions describe consistently, for unit tests of the classes that map rows to replicas.
  *
  * The cluster has nodes with stable host IDs and addresses, the table `ks.tab` with the int partition key `key` and
  * the int column `value`, and a token map in which the token of a key is the key itself and `replicasOf(key)` are its
  * replicas, among the nodes of the cluster. Remote nodes belong to another datacenter, which the connector ignores.
  *
  * [[connector]] is a real [[CassandraConnector]] over these sessions, so that its session cache and host mapping run
  * as in production. It closes a session as soon as nobody uses it (keepAliveMillis = 0), so that [[sessionsOpened]]
  * and [[sessionsClosed]] show when a session is acquired and released.
  *
  * @param nodes host ID and address of every node of the local datacenter, in the order the metadata lists them
  * @param remoteNodes host ID and address of every node of another datacenter */
final class ReplicaTestCluster(
    val nodes: Seq[(UUID, InetSocketAddress)],
    replicasOf: Int => Seq[UUID],
    remoteNodes: Seq[(UUID, InetSocketAddress)] = Seq.empty) {

  import ReplicaTestCluster._

  val sessionsOpened = new AtomicInteger
  val sessionsClosed = new AtomicInteger

  /** Makes preparing statements fail, from then on. */
  @volatile var prepareFailure: Option[Throwable] = None

  private val driverNodes: Seq[Node] =
    nodes.map { case (hostId, address) => node(hostId, address, "dc1", NodeDistance.LOCAL) } ++
      remoteNodes.map { case (hostId, address) => node(hostId, address, "dc2", NodeDistance.IGNORED) }

  private val nodesById: Map[UUID, Node] = driverNodes.map(node => node.getHostId -> node).toMap

  val tokenMap: TokenMap = new KeyTokenMap(key => replicasOf(key).flatMap(nodesById.get))

  private val metadata: Metadata = {
    val metadata = mock(classOf[Metadata])
    val nodesInOrder = new java.util.LinkedHashMap[UUID, Node]()
    driverNodes.foreach(node => nodesInOrder.put(node.getHostId, node))
    when(metadata.getNodes).thenReturn(nodesInOrder)
    when(metadata.getTokenMap).thenReturn(Optional.of(tokenMap))
    when(metadata.getKeyspaces).thenReturn(Collections.singletonMap[CqlIdentifier, KeyspaceMetadata](Keyspace, keyspace))
    metadata
  }

  val connector: CassandraConnector = CassandraConnector(CassandraConnectorConf(
    contactInfo = IpBasedContactInfo(Set(nodes.head._2)),
    keepAliveMillis = 0,
    connectionFactory = Factory(register(this))))

  private def newSession(): CqlSession = {
    sessionsOpened.incrementAndGet()
    val session = mock(classOf[CqlSession])
    val context = mock(classOf[DriverContext])
    when(context.getProtocolVersion).thenReturn(ProtocolVersion.DEFAULT)
    when(session.getContext).thenReturn(context)
    when(session.getMetadata).thenReturn(metadata)
    when(session.refreshSchema()).thenReturn(metadata)
    when(session.prepare(anyString())).thenAnswer(new Answer[PreparedStatement] {
      override def answer(invocation: InvocationOnMock): PreparedStatement = prepareFailure match {
        case Some(failure) => throw failure
        case None => keyStatement
      }
    })
    // NodeAddresses reads system.peers
    val noPeers = mock(classOf[ResultSet])
    when(noPeers.all()).thenReturn(Collections.emptyList[Row]())
    when(session.execute(anyString())).thenReturn(noPeers)
    doAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock): AnyRef = {
        sessionsClosed.incrementAndGet()
        null
      }
    }).when(session).close()
    session
  }
}

object ReplicaTestCluster {

  val Keyspace: CqlIdentifier = CqlIdentifier.fromInternal("ks")
  val Table: CqlIdentifier = CqlIdentifier.fromInternal("tab")

  private val keyColumn =
    new DefaultColumnMetadata(Keyspace, Table, CqlIdentifier.fromInternal("key"), DataTypes.INT, false)
  private val valueColumn =
    new DefaultColumnMetadata(Keyspace, Table, CqlIdentifier.fromInternal("value"), DataTypes.INT, false)

  private val table = new DefaultTableMetadata(
    Keyspace,
    Table,
    UUID.fromString("00000000-0000-0000-0000-0000000000ab"),
    false,
    false,
    List[ColumnMetadata](keyColumn).asJava,
    Collections.emptyMap[ColumnMetadata, ClusteringOrder](),
    Map[CqlIdentifier, ColumnMetadata](keyColumn.getName -> keyColumn, valueColumn.getName -> valueColumn).asJava,
    Collections.emptyMap[CqlIdentifier, AnyRef](),
    Collections.emptyMap[CqlIdentifier, IndexMetadata]())

  private val keyspace = new DefaultKeyspaceMetadata(
    Keyspace,
    true,
    false,
    Map("class" -> "org.apache.cassandra.locator.SimpleStrategy", "replication_factor" -> "2").asJava,
    Collections.emptyMap[CqlIdentifier, UserDefinedType](),
    Collections.singletonMap[CqlIdentifier, TableMetadata](Table, table),
    Collections.emptyMap[CqlIdentifier, ViewMetadata](),
    Collections.emptyMap[FunctionSignature, FunctionMetadata](),
    Collections.emptyMap[FunctionSignature, AggregateMetadata]())

  /** The statement the connector prepares to compute routing keys: one variable, the partition key. */
  private val keyStatement: PreparedStatement = {
    val keySpec = new ColumnSpec("ks", "tab", "key", 0, RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT))
    new DefaultPreparedStatement(
      ByteBuffer.wrap(Array[Byte](1)),
      "SELECT key FROM ks.tab WHERE key = :key",
      DefaultColumnDefinitions.valueOf(List[ColumnDefinition](new DefaultColumnDefinition(keySpec, null)).asJava),
      List[Integer](0).asJava,
      null,
      DefaultColumnDefinitions.valueOf(List.empty[ColumnDefinition].asJava),
      Keyspace,
      null,
      Collections.emptyMap[String, ByteBuffer](),
      null,
      null,
      Table,
      null,
      null,
      Collections.emptyMap[String, ByteBuffer](),
      java.lang.Boolean.TRUE,
      null,
      null,
      0,
      null,
      null,
      false,
      CodecRegistry.DEFAULT,
      ProtocolVersion.DEFAULT,
      RequestRoutingType.REGULAR)
  }

  private def node(hostId: UUID, address: InetSocketAddress, dc: String, distance: NodeDistance): Node = {
    val endPoint = mock(classOf[EndPoint])
    when(endPoint.resolve()).thenReturn(address)
    val node = mock(classOf[Node])
    when(node.getHostId).thenReturn(hostId)
    when(node.getEndPoint).thenReturn(endPoint)
    when(node.getDatacenter).thenReturn(dc)
    when(node.getDistance).thenReturn(distance)
    when(node.getState).thenReturn(NodeState.UP)
    node
  }

  /** A token map in which the token of a partition key is the key itself (an int). */
  private final class KeyTokenMap(replicasOf: Int => Seq[Node]) extends TokenMap {
    private def key(partitionKey: ByteBuffer): Int = partitionKey.duplicate().getInt

    private def key(token: Token): Int = token match {
      case token: Murmur3Token => token.getValue.toInt
      case other => throw new IllegalArgumentException(s"Unexpected token $other")
    }

    override def newToken(partitioner: Partitioner, partitionKey: ByteBuffer*): Token =
      new Murmur3Token(key(partitionKey.head).toLong)

    override def getReplicasList(
        keyspace: CqlIdentifier,
        partitioner: Partitioner,
        partitionKey: ByteBuffer): java.util.List[Node] = replicasOf(key(partitionKey)).asJava

    override def getReplicas(keyspace: CqlIdentifier, token: Token): java.util.Set[Node] =
      replicasOf(key(token)).toSet.asJava

    override def format(token: Token): String = token.toString
    override def getPartitionerName: String = "org.apache.cassandra.dht.Murmur3Partitioner"
    override def parse(tokenString: String): Token = throw new UnsupportedOperationException
    override def newTokenRange(start: Token, end: Token): TokenRange = throw new UnsupportedOperationException
    override def getTokenRanges: java.util.Set[TokenRange] = throw new UnsupportedOperationException
    override def getTokenRanges(node: Node): java.util.Set[TokenRange] = throw new UnsupportedOperationException
    override def getTokenRanges(keyspace: CqlIdentifier, replica: Node): java.util.Set[TokenRange] =
      throw new UnsupportedOperationException
    override def getPartitioner: Partitioner = throw new UnsupportedOperationException
  }

  private val clusters = TrieMap.empty[Int, ReplicaTestCluster]
  private val ids = new AtomicInteger

  private def register(cluster: ReplicaTestCluster): Int = {
    val id = ids.incrementAndGet()
    clusters.put(id, cluster)
    id
  }

  /** Opens the sessions of the cluster registered as `id`. Serializable, as Spark serializes connectors. */
  final case class Factory(id: Int) extends CassandraConnectionFactory {
    override def createSession(conf: CassandraConnectorConf): CqlSession = clusters(id).newSession()
  }
}
