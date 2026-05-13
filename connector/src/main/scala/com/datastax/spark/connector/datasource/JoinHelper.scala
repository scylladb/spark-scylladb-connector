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

package com.datastax.spark.connector.datasource

import java.util.concurrent.Future

import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlIdentifier, CqlSession}
import com.datastax.spark.connector.cql.{TableDef, getRowBinarySize}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.{CassandraLimit, CqlWhereClause, ReadConf}
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.CqlWhereParser.{EqPredicate, InListPredicate, InPredicate, RangePredicate}
import com.datastax.spark.connector.util.{CodecRegistryUtil, CqlWhereParser, Logging}
import com.datastax.spark.connector.writer.{BoundStatementBuilder, RateLimiter, RowWriter}
import com.datastax.spark.connector.{AllColumns, CassandraRowMetadata, ColumnRef, ColumnSelector, PartitionKeyColumns, PrimaryKeyColumns, SomeColumns}

import scala.collection.mutable

object JoinHelper extends Logging {

  private def bindMarker(columnName: String): String =
    s":${CqlIdentifier.fromInternal(columnName).asCql(true)}"

  /**
   * Determines which join columns are partition key columns and which are clustering columns.
   * Returns (partitionKeyJoinColumns, clusteringJoinColumns) in table definition order.
   */
  def splitJoinColumns(tableDef: TableDef, joinColumnNames: Seq[ColumnRef]): (Seq[ColumnRef], Seq[ColumnRef]) = {
    val partitionKeyNames = tableDef.partitionKey.map(_.columnName).toSet
    val clusteringNames = tableDef.clusteringColumns.map(_.columnName).toSet
    val joinColNames = joinColumnNames.map(_.columnName).toSet

    val pkCols = tableDef.partitionKey
      .filter(c => joinColNames.contains(c.columnName))
      .map(c => joinColumnNames.find(_.columnName == c.columnName).get)
    val ckCols = tableDef.clusteringColumns
      .filter(c => joinColNames.contains(c.columnName))
      .map(c => joinColumnNames.find(_.columnName == c.columnName).get)

    (pkCols, ckCols)
  }

  /**
   * Generates a CQL query string with IN clause for the last clustering column in the join.
   * Used for batching multiple left-side rows that share the same partition key.
   *
   * @param inClauseSize number of placeholders in the IN clause
   */
  def getJoinInQueryString(
    tableDef: TableDef,
    joinColumns: Seq[ColumnRef],
    queryParts: CqlQueryParts,
    inClauseSize: Int): String = {

    val (pkCols, ckCols) = splitJoinColumns(tableDef, joinColumns)
    require(ckCols.nonEmpty, "IN clause batching requires at least one clustering column in the join")
    require(inClauseSize >= 1, s"IN clause size must be >= 1, got $inClauseSize")

    val columns = queryParts.selectedColumnRefs.map(_.cql).mkString(", ")
    val limitClause = CassandraLimit.limitToClause(queryParts.limitClause)
    val orderBy = queryParts.clusteringOrder.map(_.toCql(tableDef)).getOrElse("")

    // Partition key columns use = :name
    val pkWhere = pkCols.map(c =>
      s"${CqlIdentifier.fromInternal(c.columnName).asCql(true)} = ${bindMarker(c.columnName)}")

    // All clustering columns except the last use = :name
    val ckEqWhere = ckCols.init.map(c =>
      s"${CqlIdentifier.fromInternal(c.columnName).asCql(true)} = ${bindMarker(c.columnName)}")

    // Last clustering column uses IN (?, ?, ...)
    val lastCk = ckCols.last
    val inPlaceholders = Seq.fill(inClauseSize)("?").mkString(", ")
    val ckInWhere = s"${CqlIdentifier.fromInternal(lastCk.columnName).asCql(true)} IN ($inPlaceholders)"

    val joinWhere = pkWhere ++ ckEqWhere :+ ckInWhere
    val filter = (queryParts.whereClause.predicates ++ joinWhere).mkString(" AND ")
    val quotedKeyspaceName = CqlIdentifier.fromInternal(tableDef.keyspaceName).asCql(true)
    val quotedTableName = CqlIdentifier.fromInternal(tableDef.tableName).asCql(true)
    val query =
      s"SELECT $columns " +
        s"FROM $quotedKeyspaceName.$quotedTableName " +
        s"WHERE $filter $orderBy $limitClause"
    logDebug(s"IN-clause batch query: $query")
    query
  }

  def joinColumnNames(joinColumns: ColumnSelector, tableDef: TableDef): Seq[ColumnRef] = joinColumns match {
    case AllColumns => throw new IllegalArgumentException(
      "Unable to join against all columns in a Cassandra Table. Only primary key columns allowed."
    )
    case PrimaryKeyColumns =>
      tableDef.primaryKey.map(col => col.columnName: ColumnRef)
    case PartitionKeyColumns =>
      tableDef.partitionKey.map(col => col.columnName: ColumnRef)
    case SomeColumns(cs@_*) =>
      ScanHelper.checkColumnsExistence(cs, tableDef)
      cs.map {
        case c: ColumnRef => c
        case _ => throw new IllegalArgumentException(
          "Unable to join against unnamed columns. No CQL Functions allowed."
        )
      }
  }

  def getJoinQueryString(
    tableDef: TableDef,
    joinColumns: Seq[ColumnRef],
    queryParts: CqlQueryParts) = {

    val whereClauses = queryParts.whereClause.predicates.flatMap(CqlWhereParser.parse)
    val joinColumnNames = joinColumns.map(_.columnName)

    val joinColumnPredicates = whereClauses.collect {
      case EqPredicate(c, _) if joinColumnNames.contains(c) => c
      case InPredicate(c) if joinColumnNames.contains(c) => c
      case InListPredicate(c, _) if joinColumnNames.contains(c) => c
      case RangePredicate(c, _, _) if joinColumnNames.contains(c) => c
    }.toSet

    require(
      joinColumnPredicates.isEmpty,
      s"""Columns specified in both the join on clause and the where clause.
         |Partition key columns are always part of the join clause.
         |Columns in both: ${joinColumnPredicates.mkString(", ")}""".stripMargin
    )


    logDebug("Generating Single Key Query Prepared Statement String")
    logDebug(s"SelectedColumns : ${queryParts.selectedColumnRefs} -- JoinColumnNames : $joinColumnNames")
    val columns = queryParts.selectedColumnRefs.map(_.cql).mkString(", ")
    val joinWhere = joinColumnNames.map { name =>
      s"${CqlIdentifier.fromInternal(name).asCql(true)} = ${bindMarker(name)}"
    }
    val limitClause = CassandraLimit.limitToClause(queryParts.limitClause)
    val orderBy = queryParts.clusteringOrder.map(_.toCql(tableDef)).getOrElse("")
    val filter = (queryParts.whereClause.predicates ++ joinWhere).mkString(" AND ")
    val quotedKeyspaceName = CqlIdentifier.fromInternal(tableDef.keyspaceName).asCql(true)
    val quotedTableName = CqlIdentifier.fromInternal(tableDef.tableName).asCql(true)
    val query =
      s"SELECT $columns " +
        s"FROM $quotedKeyspaceName.$quotedTableName " +
        s"WHERE $filter $orderBy $limitClause"
    logDebug(s"Query : $query")
    query
  }

  def getJoinPreparedStatement(
    session: CqlSession,
    queryString: String,
    consistencyLevel: ConsistencyLevel): PreparedStatement = {

    val stmt = SimpleStatement.newInstance(queryString).setConsistencyLevel(consistencyLevel).setIdempotent(true)
    session.prepare(stmt)
  }

  def getCassandraRowMetadata(
    session: CqlSession,
    statement: PreparedStatement,
    selectedColumnRefs: IndexedSeq[ColumnRef]): CassandraRowMetadata = {

    val codecRegistry = session.getContext.getCodecRegistry
    val columnNames = selectedColumnRefs.map(_.selectedAs).toIndexedSeq
    CassandraRowMetadata.fromPreparedStatement(columnNames, statement, codecRegistry)
  }

  def getKeyBuilderStatementBuilder[L](
    session: CqlSession,
    rowWriter: RowWriter[L],
    preparedStatement: PreparedStatement,
    whereClause: CqlWhereClause): BoundStatementBuilder[L] = {

    val protocolVersion = session.getContext.getProtocolVersion
    new BoundStatementBuilder[L](rowWriter, preparedStatement, whereClause.values, protocolVersion = protocolVersion)
  }

  /** Prefetches a batchSize of elements at a time **/
  def slidingPrefetchIterator[T](it: Iterator[Future[T]], batchSize: Int): Iterator[T] = {
    val (firstElements, lastElement) = it
      .grouped(batchSize)
      .sliding(2)
      .span(_ => it.hasNext)

    (firstElements.map(_.head) ++ lastElement.flatten).flatten.map(_.get)
  }

  def requestsPerSecondRateLimiter(readConf: ReadConf) = new RateLimiter(
    readConf.readsPerSec.getOrElse(Integer.MAX_VALUE).toLong,
    readConf.readsPerSec.getOrElse(Integer.MAX_VALUE).toLong
  )

  def maybeRateLimit(readConf: ReadConf): (Row => Row) = readConf.throughputMiBPS match {
    case Some(throughput) =>
      val bytesPerSecond: Long = (throughput * 1024 * 1024).toLong
      val rateLimiter = new RateLimiter(bytesPerSecond, bytesPerSecond)
      logDebug(s"Throttling join at $bytesPerSecond bytes per second")
      (row: Row) => {
        rateLimiter.maybeSleep(getRowBinarySize(row))
        row
      }
    case None => identity[Row]
  }

  /**
   * Groups consecutive elements from an iterator by a key function, with a maximum group size.
   */
  def groupConsecutive[L](
    it: Iterator[L],
    maxGroupSize: Int,
    keyFn: L => Seq[Any]
  ): Iterator[Seq[L]] = {
    val buf: scala.collection.BufferedIterator[L] = it.buffered
    new Iterator[Seq[L]] {
      override def hasNext: Boolean = buf.hasNext

      override def next(): Seq[L] = {
        val first = buf.next()
        val firstKey = keyFn(first)
        val group = mutable.ArrayBuffer(first)
        while (buf.hasNext && group.size < maxGroupSize) {
          if (keyFn(buf.head) == firstKey) group += buf.next()
          else return group.toSeq
        }
        group.toSeq
      }
    }
  }

  /**
   * Binds a prepared IN-clause statement with partition key, clustering EQ, and IN values.
   *
   * @param group         the left-side elements sharing a partition key
   * @param preparedStmt  the IN-clause prepared statement
   * @param rowWriter     writes left-side elements to value buffers
   * @param codecRegistry codec registry for type conversions
   * @param prefixVals    where clause prefix values
   * @param pkIndices     indices of PK columns in the writer buffer
   * @param ckEqIndices   indices of clustering EQ columns (all but last CK)
   * @param lastCkIndex   index of the last clustering column in the writer buffer
   */
  def bindInClauseStatement[L](
    group: Seq[L],
    preparedStmt: PreparedStatement,
    rowWriter: RowWriter[L],
    codecRegistry: CodecRegistry,
    prefixVals: Seq[Any],
    pkIndices: Seq[Int],
    ckEqIndices: Seq[Int],
    lastCkIndex: Int
  ): BoundStatement = {
    val writerColCount = rowWriter.columnNames.size
    val buffer = Array.ofDim[Any](writerColCount)
    rowWriter.readColumnValues(group.head, buffer)

    var boundStmt = preparedStmt.bind()
    var paramIdx = 0

    def bindValue(value: Any): Unit = {
      val paramType = preparedStmt.getVariableDefinitions.get(paramIdx).getType
      val converter = ColumnType.converterToCassandra(paramType)
      val converted = converter.convert(value)
      val codec = CodecRegistryUtil.codecFor(codecRegistry, paramType, converted)
      boundStmt = boundStmt.set(paramIdx, converted, codec)
      paramIdx += 1
    }

    prefixVals.foreach(bindValue)
    pkIndices.foreach(idx => bindValue(buffer(idx)))
    ckEqIndices.foreach(idx => bindValue(buffer(idx)))
    for (elem <- group) {
      rowWriter.readColumnValues(elem, buffer)
      bindValue(buffer(lastCkIndex))
    }
    boundStmt
  }

  /** Compare two CK values that might have different runtime types (e.g. Int vs Long). */
  def ckValuesMatch(leftVal: Any, rightVal: Any): Boolean = {
    if (leftVal == rightVal) true
    else (leftVal, rightVal) match {
      case (l: Number, r: Number) =>
        l.longValue() == r.longValue() && l.doubleValue() == r.doubleValue()
      case (l: Comparable[_], r) =>
        try { l.asInstanceOf[Comparable[Any]].compareTo(r) == 0 }
        catch { case _: ClassCastException => false }
      case _ => false
    }
  }

}
