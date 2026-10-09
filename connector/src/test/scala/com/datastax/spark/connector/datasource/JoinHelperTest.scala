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

import java.nio.ByteBuffer
import java.util.{Collections => JCollections}

import com.datastax.oss.driver.api.core.{CqlIdentifier, ProtocolVersion, RequestRoutingType}
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.cql.{ColumnDefinition, ColumnDefinitions, PreparedStatement}
import com.datastax.oss.driver.internal.core.cql.{
  DefaultColumnDefinition,
  DefaultColumnDefinitions,
  DefaultPreparedStatement}
import com.datastax.oss.protocol.internal.ProtocolConstants
import com.datastax.oss.protocol.internal.response.result.{ColumnSpec, RawType}
import com.datastax.spark.connector.{ColumnName, SomeColumns}
import com.datastax.spark.connector.cql.{ClusteringColumn, ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.CqlWhereClause
import com.datastax.spark.connector.types.{IntType, TextType}
import com.datastax.spark.connector.writer.RowWriter
import org.junit.{Assert, Test}

import scala.jdk.CollectionConverters._

class JoinHelperTest {

  private val tableDef = TableDef(
    "test_ks",
    "test_table",
    Seq(ColumnDef("UserId", PartitionKeyColumn, IntType)),
    Seq.empty,
    Seq(ColumnDef("value", RegularColumn, TextType)))

  private val queryParts = CqlQueryParts(
    selectedColumnRefs = SomeColumns("UserId", "value").selectFrom(tableDef),
    whereClause = CqlWhereClause.empty)

  @Test
  def joinQueryShouldQuoteCaseSensitiveBindMarkers(): Unit = {
    val query = JoinHelper.getJoinQueryString(tableDef, Seq(ColumnName("UserId")), queryParts)

    Assert.assertTrue(query, query.contains("\"UserId\" = :\"UserId\""))
  }

  @Test
  def joinInQueryShouldQuoteCaseSensitiveBindMarkers(): Unit = {
    val tableWithClustering = tableDef.copy(
      clusteringColumns = Seq(
        ColumnDef("TenantId", ClusteringColumn(0), IntType),
        ColumnDef("GroupId", ClusteringColumn(1), IntType)))
    val inQueryParts = queryParts.copy(
      selectedColumnRefs = SomeColumns("UserId", "TenantId", "GroupId", "value").selectFrom(tableWithClustering))

    val query = JoinHelper.getJoinInQueryString(
      tableWithClustering,
      Seq(ColumnName("UserId"), ColumnName("TenantId"), ColumnName("GroupId")),
      inQueryParts,
      inClauseSize = 2)

    Assert.assertTrue(query, query.contains("\"UserId\" = :\"UserId\""))
    Assert.assertTrue(query, query.contains("\"TenantId\" = :\"TenantId\""))
    Assert.assertTrue(query, query.contains("\"GroupId\" IN (?, ?)"))
  }

  @Test
  def bindInClauseStatementShouldMarkReadStatementIdempotent(): Unit = {
    val preparedStatement = preparedStatementWithVariables(
      "where_value" -> ProtocolConstants.DataType.INT,
      "UserId" -> ProtocolConstants.DataType.INT,
      "TenantId" -> ProtocolConstants.DataType.INT,
      "GroupId" -> ProtocolConstants.DataType.INT,
      "GroupId" -> ProtocolConstants.DataType.INT)
    val rowWriter = new RowWriter[Seq[Any]] {
      override def columnNames: Seq[String] = Seq("UserId", "TenantId", "GroupId")
      override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit =
        data.zipWithIndex.foreach { case (value, index) => buffer(index) = value }
    }

    val bound = JoinHelper.bindInClauseStatement(
      Seq(Seq(1, 2, 3), Seq(1, 2, 4)),
      preparedStatement,
      rowWriter,
      CodecRegistry.DEFAULT,
      prefixVals = Seq(99),
      pkIndices = Seq(0),
      ckEqIndices = Seq(1),
      lastCkIndex = 2)

    Assert.assertEquals(java.lang.Boolean.TRUE, bound.isIdempotent)
    Assert.assertEquals(99, bound.getInt(0))
    Assert.assertEquals(1, bound.getInt(1))
    Assert.assertEquals(2, bound.getInt(2))
    Assert.assertEquals(3, bound.getInt(3))
    Assert.assertEquals(4, bound.getInt(4))
  }

  private def columnDefinitions(columns: (String, Int)*): ColumnDefinitions = {
    DefaultColumnDefinitions.valueOf(columns.zipWithIndex.map { case ((name, dataType), index) =>
      val columnSpec = new ColumnSpec("ks", "tab", name, index, RawType.PRIMITIVES.get(dataType))
      new DefaultColumnDefinition(columnSpec, null).asInstanceOf[ColumnDefinition]
    }.toList.asJava)
  }

  private def preparedStatementWithVariables(columns: (String, Int)*): PreparedStatement = {
    new DefaultPreparedStatement(
      ByteBuffer.wrap(Array[Byte](1)),
      "test query",
      columnDefinitions(columns: _*),
      JCollections.emptyList[java.lang.Integer](),
      null,
      columnDefinitions(),
      CqlIdentifier.fromInternal("ks"),
      null,
      JCollections.emptyMap[String, ByteBuffer](),
      null,
      null,
      CqlIdentifier.fromInternal("tab"),
      null,
      null,
      JCollections.emptyMap[String, ByteBuffer](),
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
}
