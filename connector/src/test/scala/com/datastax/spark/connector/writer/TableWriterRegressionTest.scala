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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.{Collections => JCollections}

import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession, ProtocolVersion}
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.cql.{BoundStatement, ColumnDefinition, ColumnDefinitions, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.internal.core.cql.{DefaultColumnDefinition, DefaultColumnDefinitions, DefaultPreparedStatement}
import com.datastax.oss.protocol.internal.ProtocolConstants
import com.datastax.oss.protocol.internal.response.result.{ColumnSpec, RawType}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{CounterType, IntType, ListType, SetType, TextType, UUIDType}
import org.apache.spark.TaskContext
import org.junit.{Assert, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import scala.jdk.CollectionConverters._

class TableWriterRegressionTest {

  case class RowWithTtlPlaceholder(pk: Int, ck: Int, value: String, ttl_col: Int)
  case class DeleteKeyWithTimestampOnly(pk: Int, ck: Int, write_ts: Long)

  private def createConnector(): CassandraConnector = {
    val contactInfo = IpBasedContactInfo(Set(new InetSocketAddress("127.0.0.1", 9042)))
    val connectorConf = CassandraConnectorConf(contactInfo)
    new CassandraConnector(connectorConf)
  }

  private def createTableDef(regularColumns: Seq[ColumnDef] = Seq.empty): TableDef = {
    val pk = ColumnDef("pk", PartitionKeyColumn, IntType)
    val ck = ColumnDef("ck", ClusteringColumn(0), IntType)
    val columns = if (regularColumns.nonEmpty) regularColumns else Seq(ColumnDef("value", RegularColumn, TextType))
    TableDef("test_ks", "test_table", Seq(pk), Seq(ck), columns)
  }

  private def assertSerializable(value: AnyRef): Unit = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
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
      false)
  }

  private def connectorReturning(preparedStatement: PreparedStatement): CassandraConnector = {
    val session = mock(classOf[CqlSession])
    val context = mock(classOf[DriverContext])
    when(context.getProtocolVersion).thenReturn(ProtocolVersion.DEFAULT)
    when(session.getContext).thenReturn(context)
    when(session.prepare(any(classOf[SimpleStatement]))).thenReturn(preparedStatement)

    new CassandraConnector(createConnector().conf) {
      override def openSession(): CqlSession = session
      override def withSessionDo[T](code: CqlSession => T): T = code(session)
    }
  }

  private def asyncWriterForQuery[T](
      writer: TableWriter[T],
      query: String,
      isDeleteStatement: Boolean): AsyncStatementWriter[T] = {
    val method = writer.getClass.getDeclaredMethods.find { method =>
      method.getName.contains("getAsyncWriterInternal") && method.getParameterTypes.length == 3
    }.get
    method.setAccessible(true)
    method.invoke(
      writer,
      query,
      None,
      java.lang.Boolean.valueOf(isDeleteStatement)).asInstanceOf[AsyncStatementWriter[T]]
  }

  private def asyncWriterForDeleteCompatibilityPath(
      writer: TableWriter[Seq[Any]]): AsyncStatementWriter[Seq[Any]] = {
    asyncWriterForQuery(
      writer,
      "DELETE FROM test_ks.test_table WHERE \"pk\" = :\"pk\"",
      isDeleteStatement = true)
  }

  @Test
  def legacyDeleteWithPerRowTtlShouldNotRequirePlaceholderWhenKeyColumnsOmitIt(): Unit = {
    val writer = TableWriter[(Int, Int)](
      createConnector(), createTableDef(), PrimaryKeyColumns, WriteConf(ttl = TTLOption.perRow("ignored_ttl")),
      checkPartitionKey = true, partitions = Array.empty, tokenRangeAcc = None)

    Assert.assertEquals(Vector("pk", "ck"), writer.columnNames)
    assertSerializable((writer.delete(SomeColumns()) _).asInstanceOf[(TaskContext, Iterator[(Int, Int)]) => Unit])
  }

  @Test
  def deleteWithPerRowTtlAndTimestampShouldNotRequireIgnoredTtlProperty(): Unit = {
    val deleteConf = WriteConf(
      ttl = TTLOption.perRow("ignored_ttl"),
      timestamp = TimestampOption.perRow("write_ts")).forDelete

    val writer = TableWriter[DeleteKeyWithTimestampOnly](
      createConnector(), createTableDef(), PrimaryKeyColumns, deleteConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    Assert.assertEquals(Vector("pk", "ck"), writer.columnNames)
    Assert.assertTrue(writer.deleteQueryTemplate(SomeColumns()).contains("USING TIMESTAMP :write_ts"))
  }

  @Test
  def insertQueryShouldRejectUnboundSyntheticPerRowTtlPlaceholder(): Unit = {
    val writer = TableWriter[(Int, Int)](
      createConnector(), createTableDef(), PrimaryKeyColumns, WriteConf(ttl = TTLOption.perRow("ttl_col")),
      checkPartitionKey = false, partitions = Array.empty, tokenRangeAcc = None)

    try {
      writer.queryTemplateUsingInsert
      Assert.fail("Expected INSERT generation to reject an unbound per-row TTL placeholder")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(e.getMessage.contains("INSERT"))
        Assert.assertTrue(e.getMessage.contains("ttl_col"))
    }
  }

  @Test
  def boundStatementBuilderShouldBindDuplicateMarkersAndCountEveryOccurrence(): Unit = {
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
  def boundStatementBuilderShouldNotOverwritePrefixValuesWithDuplicateMarkerNames(): Unit = {
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
  def boundStatementBuilderShouldKeepQuotedMarkerCaseDistinct(): Unit = {
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
  def autoTimestampBindingShouldKeepQuotedMarkerCaseDistinct(): Unit = {
    val preparedStatement = preparedStatementWithVariables(
      "Connector_autots" -> ProtocolConstants.DataType.VARCHAR,
      TableWriter.AutoTimestampParam -> ProtocolConstants.DataType.BIGINT)
    val rowWriter = new RowWriter[Seq[Any]] {
      override def columnNames: Seq[String] = Seq("Connector_autots")
      override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit = buffer(0) = data.head
    }

    val bound = new BoundStatementBuilder(
      rowWriter,
      preparedStatement,
      protocolVersion = ProtocolVersion.DEFAULT,
      autoTimestampParam = Some(TableWriter.AutoTimestampParam))
      .bind(Seq("user-value"))
      .stmt

    Assert.assertEquals("user-value", bound.getString(0))
    Assert.assertTrue(bound.getLong(1) > 0L)
  }

  @Test
  def counterTableUpdateQueryShouldNotContainUsingTTL(): Unit = {
    val connector = createConnector()
    val counter = ColumnDef("c", RegularColumn, CounterType)
    val pk = ColumnDef("pk", PartitionKeyColumn, IntType)
    val tableDef = TableDef("test_ks", "test_counters", Seq(pk), Seq.empty, Seq(counter))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(ttl = TTLOption.constant(100)),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertFalse("Counter UPDATE should not contain USING TTL", query.contains("USING TTL"))
    Assert.assertFalse("Counter UPDATE should not contain USING TIMESTAMP", query.contains("USING TIMESTAMP"))
  }

  @Test
  def insertQueryShouldQuotePerRowOptionPlaceholders(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns,
      WriteConf(ttl = TTLOption.perRow("ttl-column"), timestamp = TimestampOption.perRow("write-ts")),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingInsert
    Assert.assertTrue("INSERT should quote the TTL bind marker", query.contains("TTL :\"ttl-column\""))
    Assert.assertTrue("INSERT should quote the TIMESTAMP bind marker", query.contains("TIMESTAMP :\"write-ts\""))
  }

  @Test
  def updateQueryShouldQuotePerRowOptionPlaceholders(): Unit = {
    val connector = createConnector()
    val setColumn = ColumnDef("scol", RegularColumn, SetType(TextType))
    val tableDef = createTableDef(regularColumns = Seq(setColumn))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, SomeColumns("pk", "ck", "scol".append),
      WriteConf(ttl = TTLOption.perRow("ttl-column"), timestamp = TimestampOption.perRow("write-ts")),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertTrue("UPDATE should quote the TTL bind marker", query.contains("TTL :\"ttl-column\""))
    Assert.assertTrue("UPDATE should quote the TIMESTAMP bind marker", query.contains("TIMESTAMP :\"write-ts\""))
  }

  @Test
  def deleteQueryShouldQuotePerRowOptionPlaceholders(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(timestamp = TimestampOption.perRow("write-ts")),
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(SomeColumns())
    Assert.assertTrue("DELETE should quote the TIMESTAMP bind marker", query.contains("TIMESTAMP :\"write-ts\""))
  }

  @Test
  def perRowOptionPlaceholdersShouldQuoteReservedKeywords(): Unit = {
    val connector = createConnector()
    val setColumn = ColumnDef("scol", RegularColumn, SetType(TextType))
    val tableDef = createTableDef(regularColumns = Seq(setColumn))

    val insertWriter = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(ttl = TTLOption.perRow("select")),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)
    Assert.assertTrue("INSERT should quote reserved TTL bind marker",
      insertWriter.queryTemplateUsingInsert.contains("TTL :\"select\""))

    val updateWriter = TableWriter[CassandraRow](
      connector, tableDef, SomeColumns("pk", "ck", "scol".append),
      WriteConf(timestamp = TimestampOption.perRow("select")),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)
    Assert.assertTrue("UPDATE should quote reserved TIMESTAMP bind marker",
      updateWriter.queryTemplateUsingUpdate.contains("TIMESTAMP :\"select\""))

    val deleteWriter = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(timestamp = TimestampOption.perRow("select")),
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)
    Assert.assertTrue("DELETE should quote reserved TIMESTAMP bind marker",
      deleteWriter.deleteQueryTemplate(SomeColumns()).contains("TIMESTAMP :\"select\""))
  }

  @Test
  def legacyDeleteCompatibilityPathShouldMarkEmittedStatementsIdempotent(): Unit = {
    val listColumn = ColumnDef("lcol", RegularColumn, ListType(TextType))
    val tableDef = createTableDef(regularColumns = Seq(listColumn))
    implicit val rowWriterFactory: RowWriterFactory[Seq[Any]] = new RowWriterFactory[Seq[Any]] {
      override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowWriter[Seq[Any]] = {
        new RowWriter[Seq[Any]] {
          override def columnNames: Seq[String] = selectedColumns.map(_.columnName)
          override def readColumnValues(data: Seq[Any], buffer: Array[Any]): Unit =
            data.zipWithIndex.foreach { case (value, index) => buffer(index) = value }
        }
      }
    }
    val connector = connectorReturning(preparedStatementWithVariables("pk" -> ProtocolConstants.DataType.INT))
    val writer = TableWriter[Seq[Any]](
      connector, tableDef, SomeColumns("pk", "lcol".append), WriteConf(batchGroupingKey = BatchGroupingKey.None),
      checkPartitionKey = true, partitions = Array.empty, tokenRangeAcc = None)

    Assert.assertFalse("The original collection-append writer is non-idempotent", writer.isIdempotent)

    val asyncWriter = asyncWriterForDeleteCompatibilityPath(writer)
    asyncWriter.groupingBatchBuilderBase.batchRecord(Seq(1, Seq("value")))
    val deleteStatement = asyncWriter.groupingBatchBuilderBase.finish().head.stmt

    Assert.assertEquals("DELETE statements are idempotent even when the source writer is not",
      java.lang.Boolean.TRUE, deleteStatement.isIdempotent)
  }

  @Test
  def autoSelectedTtlPlaceholderShouldBindWithOptionTypeWhenNameMatchesUnselectedRealColumn(): Unit = {
    val ttlCol = ColumnDef("ttl_col", RegularColumn, UUIDType)
    val value = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(ttlCol, value))
    val preparedStatement = preparedStatementWithVariables(
      "pk" -> ProtocolConstants.DataType.INT,
      "ck" -> ProtocolConstants.DataType.INT,
      "value" -> ProtocolConstants.DataType.VARCHAR,
      "ttl_col" -> ProtocolConstants.DataType.INT)
    val connector = connectorReturning(preparedStatement)

    val writer = TableWriter[RowWithTtlPlaceholder](
      connector, tableDef, SomeColumns("pk", "ck", "value"),
      WriteConf(batchGroupingKey = BatchGroupingKey.None, ttl = TTLOption.perRow("ttl_col")),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val asyncWriter = writer.getAsyncWriter()
    asyncWriter.groupingBatchBuilderBase.batchRecord(RowWithTtlPlaceholder(1, 2, "abc", 60))
    val bound = asyncWriter.groupingBatchBuilderBase.finish().head.stmt.asInstanceOf[BoundStatement]

    Assert.assertEquals(60, bound.getInt(3))
  }

  @Test
  def explicitDeleteTtlPlaceholderShouldUseOptionTypeWhenNameMatchesRealColumn(): Unit = {
    val ttlCol = ColumnDef("ttl_col", RegularColumn, UUIDType)
    val value = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(ttlCol, value))
    val preparedStatement = preparedStatementWithVariables(
      "pk" -> ProtocolConstants.DataType.INT,
      "ck" -> ProtocolConstants.DataType.INT)
    val connector = connectorReturning(preparedStatement)

    val writer = TableWriter[(Int, Int, Int)](
      connector, tableDef, SomeColumns("pk", "ck", "ttl_col"),
      WriteConf(batchGroupingKey = BatchGroupingKey.None, ttl = TTLOption.perRow("ttl_col")).forDelete,
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val asyncWriter = asyncWriterForQuery(
      writer,
      writer.deleteQueryTemplate(SomeColumns()),
      isDeleteStatement = true)
    asyncWriter.groupingBatchBuilderBase.batchRecord((1, 2, 60))
    val bound = asyncWriter.groupingBatchBuilderBase.finish().head.stmt.asInstanceOf[BoundStatement]

    Assert.assertEquals(1, bound.getInt(0))
    Assert.assertEquals(2, bound.getInt(1))
  }

  @Test
  def explicitDeleteTimestampPlaceholderShouldUseOptionTypeWhenNameMatchesRealColumn(): Unit = {
    val tsCol = ColumnDef("write_ts", RegularColumn, UUIDType)
    val value = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(tsCol, value))
    val preparedStatement = preparedStatementWithVariables(
      "write_ts" -> ProtocolConstants.DataType.BIGINT,
      "pk" -> ProtocolConstants.DataType.INT,
      "ck" -> ProtocolConstants.DataType.INT)
    val connector = connectorReturning(preparedStatement)
    val writeTs = 1400000000000L

    val writer = TableWriter[(Int, Int, Long)](
      connector, tableDef, SomeColumns("pk", "ck", "write_ts"),
      WriteConf(batchGroupingKey = BatchGroupingKey.None, timestamp = TimestampOption.perRow("write_ts")).forDelete,
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val asyncWriter = asyncWriterForQuery(
      writer,
      writer.deleteQueryTemplate(SomeColumns()),
      isDeleteStatement = true)
    asyncWriter.groupingBatchBuilderBase.batchRecord((1, 2, writeTs))
    val bound = asyncWriter.groupingBatchBuilderBase.finish().head.stmt.asInstanceOf[BoundStatement]

    Assert.assertEquals(writeTs, bound.getLong(0))
    Assert.assertEquals(1, bound.getInt(1))
    Assert.assertEquals(2, bound.getInt(2))
  }

  @Test
  def deleteWithPerRowTtlAndTimestampShouldPreserveTuplePositionWhenKeyColumnsOmitTtl(): Unit = {
    val tableDef = createTableDef()
    val preparedStatement = preparedStatementWithVariables(
      "write_ts" -> ProtocolConstants.DataType.BIGINT,
      "pk" -> ProtocolConstants.DataType.INT,
      "ck" -> ProtocolConstants.DataType.INT)
    val connector = connectorReturning(preparedStatement)
    val writeTs = 1400000000000L

    val writer = TableWriter[(Int, Int, Int, Long)](
      connector, tableDef, PrimaryKeyColumns,
      WriteConf(
        batchGroupingKey = BatchGroupingKey.None,
        ttl = TTLOption.perRow("ignored_ttl"),
        timestamp = TimestampOption.perRow("write_ts")).forDelete,
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val asyncWriter = asyncWriterForQuery(
      writer,
      writer.deleteQueryTemplate(SomeColumns()),
      isDeleteStatement = true)
    asyncWriter.groupingBatchBuilderBase.batchRecord((1, 2, 99999, writeTs))
    val bound = asyncWriter.groupingBatchBuilderBase.finish().head.stmt.asInstanceOf[BoundStatement]

    Assert.assertEquals(writeTs, bound.getLong(0))
    Assert.assertEquals(1, bound.getInt(1))
    Assert.assertEquals(2, bound.getInt(2))
  }

  @Test
  def deleteWithPerRowTtlAndTimestampShouldNotReadIgnoredTtlFromCassandraRow(): Unit = {
    val tableDef = createTableDef()
    val preparedStatement = preparedStatementWithVariables(
      "write_ts" -> ProtocolConstants.DataType.BIGINT,
      "pk" -> ProtocolConstants.DataType.INT,
      "ck" -> ProtocolConstants.DataType.INT)
    val connector = connectorReturning(preparedStatement)
    val writeTs = 1400000000000L

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns,
      WriteConf(
        batchGroupingKey = BatchGroupingKey.None,
        ttl = TTLOption.perRow("ignored_ttl"),
        timestamp = TimestampOption.perRow("write_ts")).forDelete,
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val asyncWriter = asyncWriterForQuery(
      writer,
      writer.deleteQueryTemplate(SomeColumns()),
      isDeleteStatement = true)
    asyncWriter.groupingBatchBuilderBase.batchRecord(
      CassandraRow.fromMap(Map("pk" -> 1, "ck" -> 2, "write_ts" -> writeTs)))
    val bound = asyncWriter.groupingBatchBuilderBase.finish().head.stmt.asInstanceOf[BoundStatement]

    Assert.assertEquals(writeTs, bound.getLong(0))
    Assert.assertEquals(1, bound.getInt(1))
    Assert.assertEquals(2, bound.getInt(2))
  }
}
