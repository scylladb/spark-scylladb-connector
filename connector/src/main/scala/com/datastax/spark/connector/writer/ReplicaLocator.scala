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

package com.datastax.spark.connector.writer


import java.net.InetAddress

import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.core.metadata.TokenMap
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.DriverUtil._
import com.datastax.spark.connector.util.{DriverUtil, Logging, tableFromCassandra}
import com.datastax.spark.connector.util.PatitionKeyTools._
import org.apache.spark.TaskContext
import org.apache.spark.util.TaskCompletionListener

import scala.collection._
import scala.jdk.CollectionConverters._

/**
 * A utility class for determining the Replica Set (Ip Addresses) of a particular Cassandra Row. Used
 * by the [[com.datastax.spark.connector.RDDFunctions.keyByCassandraReplica]] method. Uses the Java
 * Driver to obtain replica information.
 *
 * Spark serializes the locator into its tasks, so the locator only keeps descriptions: the connector configuration,
 * the table definition and the row writer. It reads the token map and the node addresses from a session whenever it
 * keys data.
 */
class ReplicaLocator[T] private(
    connector: CassandraConnector,
    tableDef: TableDef,
    rowWriter: RowWriter[T]) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames

  // Fails on the Spark driver, when the locator is created, if the token map is not available
  connector.withSessionDo(ReplicaLocator.tokenMapOf)

  /** The current token map of the cluster, read from a session: a token map is not serializable, so the locator does
    * not keep one. */
  def tokenMap: TokenMap = connector.withSessionDo(ReplicaLocator.tokenMapOf)

  /**
   * Pairs each piece of data with the Cassandra Replicas which that data would be found on.
   * Replicas that share an IP address, as nodes behind a private endpoint can, count as one.
   *
   * In a Spark task, the data is keyed as it is consumed, with a session that the returned iterator acquires for the
   * first element and releases when the data is exhausted, when keying fails, or when the task completes, fails or is
   * cancelled, also if the data was only consumed in part. Outside a Spark task, all the data is keyed right away, as
   * nothing would release a session for data that is not consumed fully.
   * @param data A source of data which can be bound to a statement by BatchStatementBuilder
   * @return an Iterator over the same data keyed by the replica's ip addresses
   */
  def keyByReplicas(data: Iterator[T]): Iterator[(scala.collection.immutable.Set[InetAddress], T)] =
    keyByReplicas(data, Option(TaskContext.get()))

  private[connector] def keyByReplicas(
      data: Iterator[T],
      task: Option[TaskContext]): Iterator[(scala.collection.immutable.Set[InetAddress], T)] =
    task match {
      case Some(taskContext) =>
        new ResourceScopedIterator[CqlSession, T, (scala.collection.immutable.Set[InetAddress], T)](
          data,
          acquire = () => connector.openSession(),
          release = _.close(),
          registerRelease = release => {
            taskContext.addTaskCompletionListener(new TaskCompletionListener {
              override def onTaskCompletion(context: TaskContext): Unit = release()
            })
            ()
          },
          init = replicaKeyer)
      case None =>
        connector.withSessionDo(session => data.map(replicaKeyer(session)).toVector).iterator
    }

  /** Keys rows by the addresses of the replicas of their partition key, with the session's current metadata.
    * Resolves the address of each replica once: through a client route, that takes a DNS lookup. */
  private def replicaKeyer(session: CqlSession): T => (scala.collection.immutable.Set[InetAddress], T) = {
    val protocolVersion = session.getContext.getProtocolVersion
    val stmt = prepareDummyStatement(session, tableDef)
    val boundStmtBuilder = new BoundStatementBuilder(rowWriter, stmt, protocolVersion = protocolVersion)
    val tokenMap = ReplicaLocator.tokenMapOf(session)
    val keyspace = CqlIdentifier.fromInternal(keyspaceName)
    val addressOf = DriverUtil.addressResolver()
    row => {
      val hosts = tokenMap
        .getReplicas(keyspace, QueryUtils.getRoutingKeyOrError(boundStmtBuilder.bind(row).stmt)).asScala
        .map(node => addressOf(node)
          .getOrElse(throw new IllegalStateException(s"Unable to determine Node Broadcast Address of $node")))
        .map(_.getAddress)
        .toSet[InetAddress]
      (hosts, row)
    }
  }
}

/**
 * Helper methods for mapping a set of data to their relative locations in a Cassandra Cluster.
 */
object ReplicaLocator {
  def apply[T: RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      partitionKeyMapper: ColumnSelector): ReplicaLocator[T] = {

    val tableDef = tableFromCassandra(connector, keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef,
      partitionKeyMapper.selectFrom(tableDef)
    )
    new ReplicaLocator[T](connector, tableDef, rowWriter)
  }

  private def tokenMapOf(session: CqlSession): TokenMap =
    toOption(session.getMetadata.getTokenMap)
      .getOrElse(throw new IllegalStateException("Unable to determine Token Map Metadata"))
}
