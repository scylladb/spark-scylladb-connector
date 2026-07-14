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

import java.nio.ByteBuffer
import java.util.{Collections => JCollections}

import com.datastax.oss.driver.api.core.{CqlIdentifier, ProtocolVersion, RequestRoutingType}
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.cql.{ColumnDefinition, ColumnDefinitions, PreparedStatement}
import com.datastax.oss.driver.internal.core.cql.{DefaultColumnDefinition, DefaultColumnDefinitions, DefaultPreparedStatement}
import com.datastax.oss.protocol.internal.ProtocolConstants
import com.datastax.oss.protocol.internal.response.result.{ColumnSpec, RawType}
import org.junit.{Assert, Test}

import scala.jdk.CollectionConverters._

class BoundStatementBuilderRegressionTest {

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

  @Test
  def shouldBindDuplicateMarkersAndCountEveryOccurrence(): Unit = {
    val preparedStatement = preparedStatementWithVariables(
      "id" -> ProtocolConstants.DataType.INT,
      "ttl_col" -> ProtocolConstants.DataType.INT,
      "value" -> ProtocolConstants.DataType.VARCHAR,
      "ttl_col" -> ProtocolConstants.DataType.INT)
    val rowWriter = new RowWriter[Seq[Any]] {
      override def columnNames: Seq[String] = Seq("id", "ttl_col", "value")
      override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit =
        data.zipWithIndex.foreach { case (value, index) => buffer(index) = value }
    }

    val builder = new BoundStatementBuilder(rowWriter, preparedStatement, protocolVersion = ProtocolVersion.DEFAULT)
    val bound = builder.bind(Seq(1, 60, "abc"))
    val ttlMarkerIndices = preparedStatement.getVariableDefinitions.allIndicesOf("ttl_col").asScala

    Assert.assertEquals(2, ttlMarkerIndices.size)
    ttlMarkerIndices.foreach { index =>
      Assert.assertNotNull(bound.stmt.getBytesUnsafe(index))
    }
    Assert.assertEquals(BoundStatementBuilder.calculateDataSize(bound.stmt), bound.bytesCount)
  }

  @Test
  def shouldNotOverwritePrefixValuesWithDuplicateMarkerNames(): Unit = {
    val preparedStatement = preparedStatementWithVariables(
      "key" -> ProtocolConstants.DataType.INT,
      "key" -> ProtocolConstants.DataType.INT)
    val rowWriter = new RowWriter[Seq[Any]] {
      override def columnNames: Seq[String] = Seq("key")
      override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit = buffer(0) = data.head
    }

    val builder = new BoundStatementBuilder(
      rowWriter,
      preparedStatement,
      prefixVals = Seq(99),
      protocolVersion = ProtocolVersion.DEFAULT)
    val bound = builder.bind(Seq(7)).stmt

    Assert.assertEquals("The prefix marker must keep the value supplied through prefixVals", 99, bound.getInt(0))
    Assert.assertEquals("The row marker should use the value read from the row", 7, bound.getInt(1))
  }

  @Test
  def shouldKeepQuotedMarkerCaseDistinct(): Unit = {
    val preparedStatement = preparedStatementWithVariables(
      "Foo" -> ProtocolConstants.DataType.INT,
      "foo" -> ProtocolConstants.DataType.INT)
    val rowWriter = new RowWriter[Seq[Any]] {
      override def columnNames: Seq[String] = Seq("Foo", "foo")
      override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit =
        data.zipWithIndex.foreach { case (value, index) => buffer(index) = value }
    }

    val bound = new BoundStatementBuilder(
      rowWriter,
      preparedStatement,
      protocolVersion = ProtocolVersion.DEFAULT)
      .bind(Seq(1, 2))
      .stmt

    Assert.assertEquals(1, bound.getInt(0))
    Assert.assertEquals(2, bound.getInt(1))
  }

  @Test
  def shouldBindUnquotedMixedCaseMarkerNames(): Unit = {
    val preparedStatement = preparedStatementWithVariables(
      "id" -> ProtocolConstants.DataType.INT,
      "ttlcol" -> ProtocolConstants.DataType.INT)
    val rowWriter = new RowWriter[Seq[Any]] {
      override def columnNames: Seq[String] = Seq("id", "ttlCol")
      override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit =
        data.zipWithIndex.foreach { case (value, index) => buffer(index) = value }
    }

    val bound = new BoundStatementBuilder(
      rowWriter,
      preparedStatement,
      protocolVersion = ProtocolVersion.DEFAULT)
      .bind(Seq(1, 60))
      .stmt

    Assert.assertEquals(1, bound.getInt("id"))
    Assert.assertEquals(60, bound.getInt("ttlcol"))
  }

  @Test
  def shouldNotBindAutoTimestampMarkerUnlessRequested(): Unit = {
    val preparedStatement = preparedStatementWithVariables(
      "id" -> ProtocolConstants.DataType.INT,
      TableWriter.AutoTimestampParam -> ProtocolConstants.DataType.BIGINT)
    val rowWriter = new RowWriter[Seq[Any]] {
      override def columnNames: Seq[String] = Seq("id")
      override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit = buffer(0) = data.head
    }

    val bound = new BoundStatementBuilder(
      rowWriter,
      preparedStatement,
      protocolVersion = ProtocolVersion.DEFAULT)
      .bind(Seq(1))
      .stmt

    Assert.assertEquals(1, bound.getInt("id"))
    Assert.assertFalse(bound.isSet(TableWriter.AutoTimestampParam))
  }

  @Test
  def shouldBindAutoTimestampMarkerWhenRequested(): Unit = {
    val preparedStatement = preparedStatementWithVariables(
      "id" -> ProtocolConstants.DataType.INT,
      TableWriter.AutoTimestampParam -> ProtocolConstants.DataType.BIGINT)
    val rowWriter = new RowWriter[Seq[Any]] {
      override def columnNames: Seq[String] = Seq("id")
      override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit = buffer(0) = data.head
    }

    val bound = new BoundStatementBuilder(
      rowWriter,
      preparedStatement,
      protocolVersion = ProtocolVersion.DEFAULT,
      autoTimestampParam = Some(TableWriter.AutoTimestampParam))
      .bind(Seq(1))

    Assert.assertEquals(1, bound.stmt.getInt("id"))
    Assert.assertTrue(bound.stmt.isSet(TableWriter.AutoTimestampParam))
    Assert.assertTrue(bound.stmt.getLong(TableWriter.AutoTimestampParam) > 0L)
    Assert.assertEquals(BoundStatementBuilder.calculateDataSize(bound.stmt), bound.bytesCount)
  }
}
