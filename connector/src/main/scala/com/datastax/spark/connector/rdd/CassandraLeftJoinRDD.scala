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

package com.datastax.spark.connector.rdd

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.datasource.JoinHelper
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.writer._
import com.google.common.util.concurrent.SettableFuture
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag
import org.apache.spark.metrics.InputMetricsUpdater

import scala.util.{Failure, Success}

/**
 * An [[org.apache.spark.rdd.RDD RDD]] that will do a selecting join between `left` RDD and the specified
 * Cassandra Table This will perform individual selects to retrieve the rows from Cassandra and will take
 * advantage of RDDs that have been partitioned with the
 * [[com.datastax.spark.connector.rdd.partitioner.ReplicaPartitioner]]
 *
 * @tparam L item type on the left side of the join (any RDD)
 * @tparam R item type on the right side of the join (fetched from Cassandra)
 */
class CassandraLeftJoinRDD[L, R] (
    override val left: RDD[L],
    val keyspaceName: String,
    val tableName: String,
    val connector: CassandraConnector,
    val columnNames: ColumnSelector = AllColumns,
    val joinColumns: ColumnSelector = PartitionKeyColumns,
    val where: CqlWhereClause = CqlWhereClause.empty,
    val limit: Option[CassandraLimit] = None,
    val clusteringOrder: Option[ClusteringOrder] = None,
    val readConf: ReadConf = ReadConf(),
    manualRowReader: Option[RowReader[R]] = None,
    override val manualRowWriter: Option[RowWriter[L]] = None)(
  implicit
    val leftClassTag: ClassTag[L],
    val rightClassTag: ClassTag[R],
    @transient val rowWriterFactory: RowWriterFactory[L],
    @transient val rowReaderFactory: RowReaderFactory[R])
  extends CassandraRDD[(L, Option[R])](left.sparkContext, left.dependencies)
  with CassandraTableRowReaderProvider[R]
  with AbstractCassandraJoin[L, Option[R]] {

  override type Self = CassandraLeftJoinRDD[L, R]

  override protected val classTag = rightClassTag

  override lazy val rowReader: RowReader[R] = manualRowReader match {
    case Some(rr) => rr
    case None => rowReaderFactory.rowReader(tableDef, columnNames.selectFrom(tableDef))
  }

  override protected def copy(
    columnNames: ColumnSelector = columnNames,
    where: CqlWhereClause = where,
    limit: Option[CassandraLimit] = limit,
    clusteringOrder: Option[ClusteringOrder] = None,
    readConf: ReadConf = readConf,
    connector: CassandraConnector = connector
  ): Self = {

    new CassandraLeftJoinRDD[L, R](
      left = left,
      keyspaceName = keyspaceName,
      tableName = tableName,
      connector = connector,
      columnNames = columnNames,
      joinColumns = joinColumns,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf
    )
  }

  override def cassandraCount(): Long = {
    columnNames match {
      case SomeColumns(_) =>
        logWarning("You are about to count rows but an explicit projection has been specified.")
      case _ =>
    }

    val counts =
      new CassandraLeftJoinRDD[L, Long](
        left = left,
        connector = connector,
        keyspaceName = keyspaceName,
        tableName = tableName,
        columnNames = SomeColumns(RowCountRef),
        joinColumns = joinColumns,
        where = where,
        limit = limit,
        clusteringOrder = clusteringOrder,
        readConf = readConf
      )

    counts.map(_._2.getOrElse(0L)).reduce(_ + _)
  }

  def on(joinColumns: ColumnSelector): CassandraLeftJoinRDD[L, R] = {
    new CassandraLeftJoinRDD[L, R](
      left = left,
      connector = connector,
      keyspaceName = keyspaceName,
      tableName = tableName,
      columnNames = columnNames,
      joinColumns = joinColumns,
      where = where,
      limit = limit,
      clusteringOrder = clusteringOrder,
      readConf = readConf
    )
  }

  /**
   * Turns this CassandraLeftJoinRDD into a factory for converting other RDD's after being serialized
   */
  private[connector] def applyToRDD(left: RDD[L]): CassandraLeftJoinRDD[L, R] = {
    new CassandraLeftJoinRDD[L, R](
      left, keyspaceName, tableName, connector, columnNames, joinColumns,
      where, limit, clusteringOrder, readConf, Some(rowReader), Some(rowWriter)
    )
  }

  /** Whether IN-clause batching can be applied for this join configuration. */
  private[rdd] def canBatchJoinQueries: Boolean = {
    val (_, ckCols) = JoinHelper.splitJoinColumns(tableDef, joinColumnNames)
    ckCols.nonEmpty && readConf.joinInClauseSize > 1
  }

  private[rdd] def fetchIterator(
    session: CqlSession,
    bsb: BoundStatementBuilder[L],
    rowMetadata: CassandraRowMetadata,
    leftIterator: Iterator[L],
    metricsUpdater: InputMetricsUpdater
  ): Iterator[(L, Option[R])] = {

    if (canBatchJoinQueries) {
      fetchIteratorWithInClause(session, bsb, rowMetadata, leftIterator, metricsUpdater)
    } else {
      fetchIteratorSingle(session, bsb, rowMetadata, leftIterator, metricsUpdater)
    }
  }

  /** Original single-row-per-query fetch strategy. */
  private[rdd] def fetchIteratorSingle(
    session: CqlSession,
    bsb: BoundStatementBuilder[L],
    rowMetadata: CassandraRowMetadata,
    leftIterator: Iterator[L],
    metricsUpdater: InputMetricsUpdater
  ): Iterator[(L, Option[R])] = {
    import com.datastax.spark.connector.util.Threads.BlockingIOExecutionContext

    val queryExecutor = ConnectorReadQueryExecutor(session, readConf, connector.conf)

    def pairWithRight(left: L): SettableFuture[Iterator[(L, Option[R])]] = {
      val resultFuture = SettableFuture.create[Iterator[(L, Option[R])]]
      val leftSide = Iterator.continually(left)

      val stmt = bsb.bind(left)
        .update(_.setPageSize(readConf.fetchSizeInRows))
        .setIdempotent(true)
        .executeAs(readConf.executeAs)
      queryExecutor.executeAsync(stmt).onComplete {
        case Success(rs) =>
          val resultSet = new PrefetchingResultSetIterator(rs, None, readConf.connectorRetry)
          val iteratorWithMetrics = resultSet.map(metricsUpdater.updateMetrics)
          val throttledIterator = iteratorWithMetrics.map(maybeRateLimit)
          val rightSide = resultSet.isEmpty match {
            case true => Iterator.single(None)
            case false => throttledIterator.map(r => Some(rowReader.read(r, rowMetadata)))
          }
          resultFuture.set(leftSide.zip(rightSide))
        case Failure(throwable) =>
          resultFuture.setException(throwable)
      }

      resultFuture
    }

    val queryFutures = leftIterator.map(left => {
      requestsPerSecondRateLimiter.maybeSleep(1)
      pairWithRight(left)
    })
    JoinHelper.slidingPrefetchIterator(queryFutures, readConf.parallelismLevel).flatten
  }

  /**
   * IN-clause batching fetch strategy for left joins. Groups consecutive left-side rows
   * sharing the same partition key and issues a single IN-clause query per group.
   * Left elements with no matching results produce (left, None) pairs.
   */
  private[rdd] def fetchIteratorWithInClause(
    session: CqlSession,
    bsb: BoundStatementBuilder[L],
    rowMetadata: CassandraRowMetadata,
    leftIterator: Iterator[L],
    metricsUpdater: InputMetricsUpdater
  ): Iterator[(L, Option[R])] = {

    import com.datastax.spark.connector.util.Threads.BlockingIOExecutionContext

    val queryExecutor = ConnectorReadQueryExecutor(session, readConf, connector.conf)
    val codecRegistry = session.getContext.getCodecRegistry
    val ctx = InClauseContext(session)

    def pairSingleWithRight(left: L): SettableFuture[Iterator[(L, Option[R])]] = {
      val resultFuture = SettableFuture.create[Iterator[(L, Option[R])]]
      val stmt = bsb.bind(left)
        .update(_.setPageSize(readConf.fetchSizeInRows))
        .setIdempotent(true)
        .executeAs(readConf.executeAs)
      queryExecutor.executeAsync(stmt).onComplete {
        case Success(rs) =>
          val resultSet = new PrefetchingResultSetIterator(rs, None, readConf.connectorRetry)
          val it = resultSet.map(metricsUpdater.updateMetrics).map(maybeRateLimit)
          val rightSide = if (resultSet.isEmpty) Iterator.single(None)
            else it.map(r => Some(rowReader.read(r, rowMetadata)))
          resultFuture.set(Iterator.continually(left).zip(rightSide))
        case Failure(t) => resultFuture.setException(t)
      }
      resultFuture
    }

    def pairGroupWithRight(group: Seq[L]): SettableFuture[Iterator[(L, Option[R])]] = {
      if (group.size == 1) return pairSingleWithRight(group.head)
      val resultFuture = SettableFuture.create[Iterator[(L, Option[R])]]
      val preparedStmt = ctx.getOrPrepare(group.size)
      val ckValueToLefts = ctx.buildCkValueMap(group)
      val boundStmt = JoinHelper.bindInClauseStatement(
        group, preparedStmt, rowWriter, codecRegistry,
        where.values, ctx.pkIndices, ctx.ckEqIndices, ctx.lastCkIndex
      ).setPageSize(readConf.fetchSizeInRows)
      val richStmt = new RichBoundStatementWrapper(boundStmt).executeAs(readConf.executeAs)
      queryExecutor.executeAsync(richStmt).onComplete {
        case Success(rs) =>
          val it = new PrefetchingResultSetIterator(rs, None, readConf.connectorRetry).map(metricsUpdater.updateMetrics)
          val throttled = it.map(maybeRateLimit)
          resultFuture.set(ctx.matchLeftJoinResults(throttled, group, ckValueToLefts, rowMetadata, rowReader))
        case Failure(t) => resultFuture.setException(t)
      }
      resultFuture
    }

    val groupedIt = JoinHelper.groupConsecutive(leftIterator, readConf.joinInClauseSize, ctx.extractPk)
    val queryFutures = groupedIt.map { group =>
      requestsPerSecondRateLimiter.maybeSleep(1)
      pairGroupWithRight(group)
    }
    JoinHelper.slidingPrefetchIterator(queryFutures, readConf.parallelismLevel).flatten
  }

  /** Encapsulates state needed for IN-clause batching within a partition. */
  private case class InClauseContext(session: CqlSession) {
    private val (pkCols, ckCols) = JoinHelper.splitJoinColumns(tableDef, joinColumnNames)
    private val writerColNames = rowWriter.columnNames
    private val queryParts = CqlQueryParts(selectedColumnRefs, where, limit, clusteringOrder)
    private val stmtCache = mutable.Map.empty[Int, PreparedStatement]

    val pkIndices: Seq[Int] = pkCols.map(c => writerColNames.indexOf(c.columnName))
    val ckEqIndices: Seq[Int] = ckCols.init.map(c => writerColNames.indexOf(c.columnName))
    val lastCkIndex: Int = writerColNames.indexOf(ckCols.last.columnName)
    val lastCkColumnName: String = ckCols.last.columnName

    def getOrPrepare(size: Int): PreparedStatement = stmtCache.getOrElseUpdate(size, {
      val q = JoinHelper.getJoinInQueryString(tableDef, joinColumnNames, queryParts, size)
      JoinHelper.getJoinPreparedStatement(session, q, consistencyLevel)
    })

    def extractPk(left: L): Seq[Any] = {
      val buf = Array.ofDim[Any](writerColNames.size)
      rowWriter.readColumnValues(left, buf)
      pkIndices.map(buf(_))
    }

    def buildCkValueMap(group: Seq[L]): mutable.LinkedHashMap[Any, mutable.ArrayBuffer[L]] = {
      val map = mutable.LinkedHashMap.empty[Any, mutable.ArrayBuffer[L]]
      val buf = Array.ofDim[Any](writerColNames.size)
      for (elem <- group) {
        rowWriter.readColumnValues(elem, buf)
        map.getOrElseUpdate(buf(lastCkIndex), mutable.ArrayBuffer.empty) += elem
      }
      map
    }

    /** For left joins: collect results, then emit (left, Some(right)) or (left, None). */
    def matchLeftJoinResults(
      rows: Iterator[com.datastax.oss.driver.api.core.cql.Row],
      group: Seq[L],
      ckMap: mutable.LinkedHashMap[Any, mutable.ArrayBuffer[L]],
      rowMeta: CassandraRowMetadata,
      rReader: reader.RowReader[R]
    ): Iterator[(L, Option[R])] = {
      val ckColIdx = rowMeta.indexOfCqlColumnOrThrow(lastCkColumnName)
      val resultsByCk = mutable.LinkedHashMap.empty[Any, mutable.ArrayBuffer[R]]
      rows.foreach { row =>
        val right = rReader.read(row, rowMeta)
        val ckVal = row.getObject(ckColIdx)
        resultsByCk.getOrElseUpdate(ckVal, mutable.ArrayBuffer.empty) += right
      }
      val buf = Array.ofDim[Any](writerColNames.size)
      group.iterator.flatMap { left =>
        rowWriter.readColumnValues(left, buf)
        val leftCkVal = buf(lastCkIndex)
        val rights = resultsByCk.get(leftCkVal).orElse(
          resultsByCk.collectFirst { case (k, v) if JoinHelper.ckValuesMatch(leftCkVal, k) => v }
        )
        rights match {
          case Some(rs) => rs.iterator.map(r => (left, Some(r)))
          case None => Iterator.single((left, None))
        }
      }
    }
  }
}
