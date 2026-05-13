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

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.datastax.oss.driver.api.core.{CqlIdentifier, DefaultProtocolVersion, ProtocolVersion}
import com.datastax.spark.connector.types.{ColumnType, Unset}
import com.datastax.spark.connector.util.{CodecRegistryUtil, Logging}

import scala.jdk.CollectionConverters._

/**
 * Class for binding row-like objects into prepared statements. prefixVals
 * is used for binding constant values into each bound statement. This supports parametrized
 * .where clauses in [[com.datastax.spark.connector.rdd.CassandraJoinRDD]]
 */
private[connector] class BoundStatementBuilder[T](
    val rowWriter: RowWriter[T],
    val preparedStmt: PreparedStatement,
    val prefixVals: Seq[Any] = Seq.empty,
    val ignoreNulls: Boolean = false,
    val protocolVersion: ProtocolVersion,
    val autoTimestampParam: Option[String] = None) extends Logging {

  private val columnNames = rowWriter.columnNames.toIndexedSeq
  private val variableDefinitions = preparedStmt.getVariableDefinitions

  /** Precomputed variable indices for each column in the prepared statement.
    * Empty for columns not present in the row-bound part of the statement (option placeholders).
    * A column name can occur more than once when a real column is also used as
    * a per-row TTL/TIMESTAMP placeholder; bind every occurrence by index so
    * each marker uses its own CQL type and is included in size accounting.
    * Prefix values are bound positionally by preparedStmt.bind and must not be
    * overwritten when their marker names collide with row-bound marker names. */
  private def variableIndicesFor(name: String): Array[Int] = {
    val exactIndices = variableDefinitions.allIndicesOf(CqlIdentifier.fromInternal(name)).asScala
    val indices = if (exactIndices.nonEmpty) exactIndices else variableDefinitions.allIndicesOf(name).asScala
    indices.map(_.toInt).filter(_ >= prefixVals.size).toArray
  }

  private val varIndices: Array[Array[Int]] = columnNames.map(variableIndicesFor).toArray
  private val inStatement: Array[Boolean] = varIndices.map(_.nonEmpty)
  private val columnTypes = varIndices.map(_.map(index => variableDefinitions.get(index).getType))
  private val converters = columnTypes.map(_.map(ColumnType.converterToCassandra(_)))
  private val buffer = Array.ofDim[Any](columnNames.size)
  private val cachedCodecs = columnTypes.map(types => new Array[TypeCodec[AnyRef]](types.length))

  /** Internal auto-timestamp placeholder to bind, if this statement was generated with one. */
  private val autoTimestampIndices: Array[Int] =
    autoTimestampParam
      .map(variableIndicesFor)
      .getOrElse(Array.emptyIntArray)

  /** Monotonically incrementing microsecond counter for auto-timestamps.
    * Uses a global counter to guarantee uniqueness across all concurrent
    * BoundStatementBuilder instances within the same JVM. */
  private val autoTsCounter = BoundStatementBuilder.globalAutoTsCounter


  require(!ignoreNulls || protocolVersion.getCode >= DefaultProtocolVersion.V4.getCode,
    s"""
       |Protocol Version $protocolVersion does not support ignoring null values and leaving
       |parameters unset. This is only supported in ${DefaultProtocolVersion.V4.getCode} and greater.
    """.stripMargin)

  var logUnsetToNullWarning = false
  val UnsetToNullWarning =
    s"""Unset values can only be used with C* >= 2.2. They have been replaced
        |with nulls. Found protocol version ${protocolVersion}.
        |${DefaultProtocolVersion.V4.getCode}  or greater required"
    """.stripMargin


  private def maybeLeaveUnset(
    boundStatement: BoundStatement,
    columnName: String): Unit = protocolVersion match {
      case pv if (pv.getCode() <= DefaultProtocolVersion.V3.getCode()) => {
        boundStatement.setToNull(columnName)
        logUnsetToNullWarning = true
      }
      case _ =>
  }

  private def bindColumnNull(
    boundStatement: RichBoundStatementWrapper,
    variableIndex: Int,
    columnValue: AnyRef,
    codec: TypeCodec[AnyRef]): Unit = {

    if (columnValue == Unset || (ignoreNulls && columnValue == null)) {
      boundStatement.update(s => s.setToNull(variableIndex))
      logUnsetToNullWarning = true
    } else {
      boundStatement.update(s => s.set(variableIndex, columnValue, codec))
    }
  }

  private def bindColumnUnset(
    boundStatement: RichBoundStatementWrapper,
    variableIndex: Int,
    columnValue: AnyRef,
    codec: TypeCodec[AnyRef]): Unit = {

    if (columnValue == Unset || (ignoreNulls && columnValue == null)) {
      //Do not bind
    } else {
      boundStatement.update(s => s.set(variableIndex, columnValue, codec))
    }
  }

  /**
  * If the protocol version is greater than V3 (C* 2.2 and Greater) then
  * we can leave values in the prepared statement unset. If the version is
  * less than V3 then we need to place a `null` in the bound statement.
  */
  val bindColumn: (RichBoundStatementWrapper, Int, AnyRef, TypeCodec[AnyRef]) => Unit = protocolVersion match {
    case pv if pv.getCode() <= DefaultProtocolVersion.V3.getCode => bindColumnNull
    case _ => bindColumnUnset
  }

  private val prefixConverted = for {
    prefixIndex: Int <- prefixVals.indices
    prefixVal = prefixVals(prefixIndex)
    prefixType = preparedStmt.getVariableDefinitions.get(prefixIndex).getType
    prefixConverter =  ColumnType.converterToCassandra(prefixType)
  } yield prefixConverter.convert(prefixVal)

  /** Creates `BoundStatement` from the given data item */
  def bind(row: T): RichBoundStatementWrapper = {

    val boundStatement = new RichBoundStatementWrapper(preparedStmt.bind(prefixConverted: _*))

    rowWriter.readColumnValues(row, buffer)
    val codecRegistry = boundStatement.stmt.codecRegistry()
    var bytesCount = 0
    for (i <- columnNames.indices if inStatement(i)) {
      for (j <- varIndices(i).indices) {
        val converter = converters(i)(j)
        val variableIndex = varIndices(i)(j)
        val columnType = columnTypes(i)(j)
        val columnValue = converter.convert(buffer(i))
        val codec = {
          var c = cachedCodecs(i)(j)
          if (c == null && columnValue != Unset) {
            c = CodecRegistryUtil.codecFor(codecRegistry, columnType, columnValue)
            cachedCodecs(i)(j) = c
          }
          c
        }
        bindColumn(boundStatement, variableIndex, columnValue, codec)
        val serializedValue = boundStatement.stmt.getBytesUnsafe(variableIndex)
        if (serializedValue != null) bytesCount += serializedValue.remaining()
      }
    }

    autoTimestampIndices.foreach { index =>
      boundStatement.update(_.setLong(index, autoTsCounter.getAndIncrement()))
      bytesCount += 8 // Long = 8 bytes, not counted in the column loop above
    }

    boundStatement.bytesCount = bytesCount
    boundStatement
  }
}

private[connector] object BoundStatementBuilder {

  /** Global monotonically incrementing microsecond counter shared across all
    * BoundStatementBuilder instances in the JVM. Ensures that every bound
    * statement gets a unique timestamp even when multiple Spark tasks bind
    * concurrently. Initialized to the current wall-clock time in microseconds
    * so that timestamps stay in the same ballpark as server-assigned ones.
    *
    * Known limitations:
    * - JVM restart (OOM, preemption, speculative execution) may initialize
    *   a new counter below the previous JVM's last used value. This is
    *   acceptable because the auto-timestamp only needs to order mutations
    *   within a single batch to avoid list-mutation deduplication (issue #26);
    *   cross-JVM ordering relies on CQL last-write-wins semantics.
    * - Overflow is not a practical concern: at ~1.7e15 microseconds since
    *   epoch, Long.MaxValue (~9.2e18) provides ~584 years of headroom. */
  private[writer] val globalAutoTsCounter =
    new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis() * 1000)

  /** Calculate bound statement size in bytes. */
  def calculateDataSize(stmt: BoundStatement): Int = {
    var size = 0
    for (i <- 0 until stmt.getPreparedStatement.getVariableDefinitions.size())
      if (!stmt.isNull(i)) size += stmt.getBytesUnsafe(i).remaining()

    size
  }
}
