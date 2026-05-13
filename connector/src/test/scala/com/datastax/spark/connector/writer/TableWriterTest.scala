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

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{BigIntType, CounterType, IntType, SetType, TextType}
import org.junit.{Assert, Test}
import org.apache.spark.{Partition, TaskContext}

class TableWriterTest {

  private def createConnector(): CassandraConnector = {
    val contactInfo = IpBasedContactInfo(Set(new InetSocketAddress("127.0.0.1", 9042)))
    val connectorConf = CassandraConnectorConf(contactInfo)
    new CassandraConnector(connectorConf)
  }

  private def createTableDef(regularColumns: Seq[ColumnDef] = Seq.empty): TableDef = {
    val pk = ColumnDef("pk", PartitionKeyColumn, IntType)
    val ck = ColumnDef("ck", ClusteringColumn(0), IntType)
    TableDef("test_ks", "test_table", Seq(pk), Seq(ck), regularColumns)
  }

  private def assertSerializable(value: AnyRef): Unit = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
  }

  private class RecordingRowWriterFactory extends RowWriterFactory[CassandraRow] {
    var selectedColumns: IndexedSeq[ColumnRef] = IndexedSeq.empty

    override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowWriter[CassandraRow] = {
      this.selectedColumns = selectedColumns
      new RowWriter[CassandraRow] {
        override def columnNames: Seq[String] = selectedColumns.map(_.columnName)
        override def readColumnValues(data: CassandraRow, buffer: Array[Any]): Unit = ()
      }
    }
  }

  @Test
  def autoTimestampCounterShouldNotStartFarInTheFuture(): Unit = {
    val before = System.currentTimeMillis() * 1000
    val seed = BoundStatementBuilder.globalAutoTsCounter.get()
    val after = System.currentTimeMillis() * 1000

    Assert.assertTrue(
      s"Auto timestamp seed $seed should not be more than 100ms ahead of wall clock [$before, $after]",
      seed <= after + 100000L)
  }

  @Test
  def deleteClosureShouldBeSerializable(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val deleteFunc: (TaskContext, Iterator[CassandraRow]) => Unit = writer.deleteRows _

    assertSerializable(deleteFunc) // must not throw NotSerializableException
  }

  @Test
  def deleteClosureWithPerRowTimestampShouldBeSerializable(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()
    val writeConf = WriteConf(timestamp = TimestampOption.perRow("_ts"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val deleteFunc: (TaskContext, Iterator[CassandraRow]) => Unit = writer.deleteRows _

    assertSerializable(deleteFunc) // must not throw NotSerializableException
  }

  @Test
  def deleteWithColumnSelectorShouldRemainSourceCompatible(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), checkPartitionKey = true,
      partitions = Array.empty, tokenRangeAcc = None)

    val deleteFunc: (TaskContext, Iterator[CassandraRow]) => Unit = writer.delete(SomeColumns()) _

    assertSerializable(deleteFunc)
  }

  @Test
  def deleteMethodValueShouldRemainSourceCompatible(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), checkPartitionKey = true,
      partitions = Array.empty, tokenRangeAcc = None)

    val deleteFunc = writer.delete _

    assertSerializable(deleteFunc)
  }

  @Test
  def deleteWithPerRowTtlShouldSucceedWhenKeyColumnsOmitPlaceholder(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()
    val writeConf = WriteConf(ttl = TTLOption.perRow("ignored_ttl"))
    val deleteConf = writeConf.forDelete

    // forDelete keeps per-row TTL for positional alignment; the DELETE template omits it
    Assert.assertTrue(
      "forDelete should keep per-row TTL placeholder",
      deleteConf.optionPlaceholders.contains("ignored_ttl"))

    // Building a TableWriter with PrimaryKeyColumns and the delete conf should succeed
    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, deleteConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val deleteFunc: (TaskContext, Iterator[CassandraRow]) => Unit = writer.deleteRows _

    assertSerializable(deleteFunc)
  }

  @Test
  def deleteWithPerRowTtlShouldNotRequirePlaceholderWhenKeyColumnsOmitIt(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()
    val deleteConf = WriteConf(ttl = TTLOption.perRow("ignored_ttl")).forDelete

    val writer = TableWriter[(Int, Int)](
      connector, tableDef, PrimaryKeyColumns, deleteConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    Assert.assertEquals(Vector("pk", "ck"), writer.columnNames)
  }

  @Test
  def deleteWithPerRowTtlShouldKeepExplicitPlaceholderForTupleAlignment(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()
    val deleteConf = WriteConf(ttl = TTLOption.perRow("ignored_ttl")).forDelete

    val writer = TableWriter[(Int, Int, Int)](
      connector, tableDef, SomeColumns("pk", "ck", "ignored_ttl"), deleteConf,
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    Assert.assertEquals(Vector("pk", "ck"), writer.columnNames)
  }

  @Test
  def deleteQueryTemplateShouldRejectRegularColumnsAsKeyColumns(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(), partitionKeyOnly = false,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    try {
      writer.deleteQueryTemplate(SomeColumns())
      Assert.fail("Expected IllegalArgumentException for regular columns as key columns")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(e.getMessage.contains("Regular columns found"))
    }
  }

  @Test
  def deleteQueryTemplateWithAllColumnsShouldAutoFilterPrimaryKeyColumns(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(AllColumns)
    Assert.assertTrue("Should contain the regular column", query.contains("\"value\""))
    Assert.assertFalse("Should not contain pk column in delete spec", query.contains("\"pk\" FROM"))
    Assert.assertFalse("Should not contain ck column in delete spec", query.contains("\"ck\" FROM"))
  }

  @Test
  def deleteQueryTemplateWithAllColumnsOnPkOnlyTableShouldProduceFullRowDelete(): Unit = {
    val connector = createConnector()
    // Table with only primary key columns (no regular columns)
    val tableDef = createTableDef(regularColumns = Seq.empty)

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(AllColumns)
    // Should be a full-row DELETE (no column list before FROM)
    Assert.assertTrue("Should be a full-row DELETE", query.startsWith("DELETE FROM"))
    Assert.assertTrue("Should contain WHERE clause", query.contains("WHERE"))
  }

  @Test
  def allColumnsDeleteOnPkOnlyTableShouldNotRequireFullPrimaryKey(): Unit = {
    val tableDef = createTableDef(regularColumns = Seq.empty)

    Assert.assertFalse(TableWriter.isColumnDelete(tableDef, AllColumns))
  }

  @Test
  def allColumnsDeleteOnTableWithRegularColumnsShouldRequireFullPrimaryKey(): Unit = {
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    Assert.assertTrue(TableWriter.isColumnDelete(tableDef, AllColumns))
  }

  @Test
  def regularColumnDeleteShouldRejectPartitionKeyOnly(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))
    val deleteColumns = SomeColumns("value")

    try {
      TableWriter[CassandraRow](
        connector, tableDef, SomeColumns("pk"), WriteConf(),
        partitionKeyOnly = !TableWriter.isColumnDelete(tableDef, deleteColumns),
        partitions = Array.empty, tokenRangeAcc = None, isDelete = true)
      Assert.fail("Expected regular column delete to require the full primary key")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(e.getMessage.contains("ck"))
    }
  }

  @Test
  def legacyDeleteQueryTemplateShouldRejectNonStaticColumnDeleteWithoutFullPrimaryKey(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, SomeColumns("pk"), WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    try {
      writer.deleteQueryTemplate(SomeColumns("value"))
      Assert.fail("Expected non-static column delete to require the full primary key")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(e.getMessage.contains("full primary key"))
        Assert.assertTrue(e.getMessage.contains("ck"))
    }
  }

  @Test
  def staticColumnDeleteShouldAllowPartitionKeyOnly(): Unit = {
    val connector = createConnector()
    val staticCol = ColumnDef("static_value", StaticColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(staticCol))
    val deleteColumns = SomeColumns("static_value")

    val writer = TableWriter[CassandraRow](
      connector, tableDef, SomeColumns("pk"), WriteConf(),
      partitionKeyOnly = !TableWriter.isColumnDelete(tableDef, deleteColumns),
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val query = writer.deleteQueryTemplate(deleteColumns)
    Assert.assertTrue("DELETE should include the static column", query.contains("\"static_value\""))
    Assert.assertTrue("DELETE should use only the partition key", query.contains("WHERE \"pk\" = :\"pk\""))
    Assert.assertFalse("DELETE should not require the clustering column", query.contains("\"ck\" = :\"ck\""))
  }

  @Test
  def staticColumnDeleteWithPrimaryKeyColumnsShouldUseOnlyPartitionKey(): Unit = {
    val connector = createConnector()
    val staticCol = ColumnDef("static_value", StaticColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(staticCol))
    val deleteColumns = SomeColumns("static_value")

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(),
      partitionKeyOnly = !TableWriter.isColumnDelete(tableDef, deleteColumns),
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val query = writer.deleteQueryTemplate(deleteColumns)
    Assert.assertTrue("DELETE should include the static column", query.contains("\"static_value\""))
    Assert.assertTrue("DELETE should use the partition key", query.contains("WHERE \"pk\" = :\"pk\""))
    Assert.assertFalse("DELETE should not include the clustering column for a static-only delete",
      query.contains("\"ck\" = :\"ck\""))
  }

  @Test
  def allColumnsDeleteOnStaticOnlyTableShouldAllowPartitionKeyOnly(): Unit = {
    val connector = createConnector()
    val staticCol = ColumnDef("static_value", StaticColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(staticCol))
    val deleteColumns = AllColumns

    val writer = TableWriter[CassandraRow](
      connector, tableDef, SomeColumns("pk"), WriteConf(),
      partitionKeyOnly = !TableWriter.isColumnDelete(tableDef, deleteColumns),
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val query = writer.deleteQueryTemplate(deleteColumns)
    Assert.assertTrue("DELETE should include the static column", query.contains("\"static_value\""))
    Assert.assertTrue("DELETE should use only the partition key", query.contains("WHERE \"pk\" = :\"pk\""))
    Assert.assertFalse("DELETE should not require the clustering column", query.contains("\"ck\" = :\"ck\""))
  }

  @Test
  def deleteWithPerRowTimestampAndPrimaryKeyColumnsShouldIncludePlaceholder(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()
    val writeConf = WriteConf(timestamp = TimestampOption.perRow("_ts"))

    // PrimaryKeyColumns would not normally select the synthetic "_ts" column.
    // The fix ensures it is automatically included.
    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(SomeColumns())
    Assert.assertTrue("Delete query should contain USING TIMESTAMP :\"_ts\"",
      query.contains("USING TIMESTAMP :\"_ts\""))
  }

  @Test
  def deleteWithExplicitRealTimestampPlaceholderShouldNotTreatPlaceholderAsKeyColumn(): Unit = {
    val connector = createConnector()
    val writeTs = ColumnDef("write_ts", RegularColumn, BigIntType)
    val tableDef = createTableDef(regularColumns = Seq(writeTs))
    val writeConf = WriteConf(timestamp = TimestampOption.perRow("write_ts"))

    val writer = TableWriter[(Int, Int, Long)](
      connector, tableDef, SomeColumns("pk", "ck", "write_ts"), writeConf,
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val query = writer.deleteQueryTemplate(SomeColumns())

    Assert.assertEquals(Vector("pk", "ck"), writer.columnNames)
    Assert.assertTrue("Delete query should use the real column as the per-row timestamp placeholder",
      query.contains("USING TIMESTAMP :write_ts"))
    Assert.assertFalse("Timestamp placeholder should not be used as a DELETE key column",
      query.contains("write_ts\" = :\"write_ts"))
  }

  @Test
  def directDeleteQueryShouldNotTreatExplicitRealTimestampPlaceholderAsKeyColumn(): Unit = {
    val connector = createConnector()
    val writeTs = ColumnDef("write_ts", RegularColumn, BigIntType)
    val tableDef = createTableDef(regularColumns = Seq(writeTs))
    val writeConf = WriteConf(timestamp = TimestampOption.perRow("write_ts"))

    val writer = TableWriter[(Int, Int, Long)](
      connector, tableDef, SomeColumns("pk", "ck", "write_ts"), writeConf,
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(SomeColumns())

    Assert.assertTrue("Delete query should use the real column as the per-row timestamp placeholder",
      query.contains("USING TIMESTAMP :write_ts"))
    Assert.assertFalse("Timestamp placeholder should not be used as a DELETE key column",
      query.contains("write_ts\" = :\"write_ts"))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def deleteQueryTemplateShouldRejectCollectionColumnNameInDeleteColumns(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    writer.deleteQueryTemplate(SomeColumns("value".append))
  }

  @Test
  def deleteQueryTemplateShouldAllowCollectionOverwriteInDeleteColumns(): Unit = {
    val connector = createConnector()
    val setColumn = ColumnDef("scol", RegularColumn, SetType(TextType))
    val tableDef = createTableDef(regularColumns = Seq(setColumn))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(SomeColumns("scol".overwrite))
    Assert.assertTrue("DELETE should include the collection column", query.contains("\"scol\""))
  }

  @Test
  def deleteQueryTemplateShouldRejectExplicitPrimaryKeyColumnsAsDeleteColumns(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    try {
      writer.deleteQueryTemplate(SomeColumns("pk"))
      Assert.fail("Expected IllegalArgumentException for primary key columns in deleteColumns")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(e.getMessage.contains("Primary key columns cannot be specified as delete columns"))
    }
  }

  @Test
  def insertQueryTemplateShouldIncludeIfNotExistsWithTtlAndTimestamp(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns,
      WriteConf(ifNotExists = true, ttl = TTLOption.constant(100),
        timestamp = TimestampOption.constant(1400000000000L)),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingInsert
    Assert.assertTrue("INSERT should contain IF NOT EXISTS",
      query.contains("IF NOT EXISTS"))
    Assert.assertTrue("INSERT should contain USING TTL 100 AND TIMESTAMP 1400000000000",
      query.contains("USING TTL 100 AND TIMESTAMP 1400000000000"))
    // IF NOT EXISTS must appear before USING clause in valid CQL
    val ifNotExistsIdx = query.indexOf("IF NOT EXISTS")
    val usingIdx = query.indexOf("USING")
    Assert.assertTrue("IF NOT EXISTS should appear before USING clause",
      ifNotExistsIdx < usingIdx)
  }

  @Test
  def forDeleteShouldStripIfNotExistsIgnoreNullsAndStaticTtl(): Unit = {
    val writeConf = WriteConf(
      ifNotExists = true,
      ignoreNulls = true,
      ttl = TTLOption.constant(100),
      timestamp = TimestampOption.constant(1400000000000L))
    val deleteConf = writeConf.forDelete

    Assert.assertFalse("forDelete should strip ifNotExists", deleteConf.ifNotExists)
    Assert.assertFalse("forDelete should strip ignoreNulls", deleteConf.ignoreNulls)
    // Static TTL is stripped because CQL DELETE does not support TTL
    Assert.assertEquals("forDelete should strip static TTL", TTLOption.defaultValue, deleteConf.ttl)
    Assert.assertEquals("forDelete should preserve timestamp",
      TimestampOption.constant(1400000000000L), deleteConf.timestamp)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldThrowWhenTtlPlaceholderCollidesWithAutoTimestampParam(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns,
      WriteConf(ttl = TTLOption.perRow(TableWriter.AutoTimestampParam)),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    writer.queryTemplateUsingUpdate
  }

  @Test
  def updateQueryShouldAlwaysIncludeAutoTimestamp(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertTrue("UPDATE should contain auto-timestamp even without collection behaviors",
      query.contains(s"USING TIMESTAMP :${TableWriter.AutoTimestampParam}"))
  }

  @Test
  def deleteQueryTemplateShouldGiveDeleteSpecificErrorForMissingColumns(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    try {
      writer.deleteQueryTemplate(SomeColumns("nonexistent"))
      Assert.fail("Expected IllegalArgumentException for nonexistent delete column")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue("Error should mention DELETE",
          e.getMessage.contains("DELETE"))
        Assert.assertTrue("Error should mention the missing column",
          e.getMessage.contains("nonexistent"))
    }
  }

  @Test
  def autotsShouldNotCollideWithUserColumnNamedAutots(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("autots", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    // Should NOT throw — the internal parameter name no longer collides with "autots"
    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertTrue("UPDATE should contain the user column 'autots'",
      query.contains("\"autots\""))
  }

  @Test
  def insertQueryShouldAllowUserColumnNamedAutoTimestampParam(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef(TableWriter.AutoTimestampParam, RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingInsert
    Assert.assertTrue("INSERT should contain the user column named like the internal auto timestamp marker",
      query.contains(s"\"${TableWriter.AutoTimestampParam}\""))
  }

  @Test
  def updateQueryShouldAllowUserColumnNamedAutoTimestampParamWithExplicitTimestamp(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef(TableWriter.AutoTimestampParam, RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))
    val timestamp = 1400000000000L

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(timestamp = TimestampOption.constant(timestamp)),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertTrue("UPDATE should contain the user column named like the internal auto timestamp marker",
      query.contains(s"\"${TableWriter.AutoTimestampParam}\""))
    Assert.assertTrue("UPDATE should use the explicit timestamp instead of the internal marker",
      query.contains(s"USING TIMESTAMP $timestamp"))
  }

  @Test
  def tableDefApplyOverloadShouldRemainSourceCompatible(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), checkPartitionKey = true,
      partitions = Array.empty, tokenRangeAcc = None)

    Assert.assertEquals(Vector("pk", "ck"), writer.columnNames)
  }

  @Test
  def keyspaceApplyOverloadShouldKeepLegacyBinarySignature(): Unit = {
    val legacySignature = Seq(
      classOf[CassandraConnector],
      classOf[String],
      classOf[String],
      classOf[ColumnSelector],
      classOf[WriteConf],
      java.lang.Boolean.TYPE,
      classOf[Array[Partition]],
      classOf[Option[_]],
      classOf[RowWriterFactory[_]]).map(_.getName)

    val hasLegacySignature = TableWriter.getClass.getMethods.exists { method =>
      method.getName == "apply" && method.getParameterTypes.map(_.getName).toSeq == legacySignature
    }

    Assert.assertTrue(
      "TableWriter.apply(connector, keyspace, table, columns, writeConf, checkPartitionKey, partitions, tokenRangeAcc) " +
        "must remain available for callers compiled against previous releases",
      hasLegacySignature)
  }

  @Test
  def forDeleteShouldRejectAllColumnsAsKeyColumns(): Unit = {
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    try {
      TableWriter.validateDeleteSelectors(tableDef, SomeColumns(), AllColumns)
      Assert.fail("Expected AllColumns to be rejected when the table has non-primary-key columns")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(e.getMessage.contains("AllColumns cannot be used as keyColumns"))
    }
  }

  @Test
  def forDeleteShouldAllowAllColumnsAsKeyColumnsOnPrimaryKeyOnlyTable(): Unit = {
    val tableDef = createTableDef()

    TableWriter.validateDeleteSelectors(tableDef, SomeColumns(), AllColumns)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def forDeleteShouldRejectPrimaryKeyColumnsAsDeleteColumns(): Unit = {
    TableWriter.validateDeleteSelectors(createTableDef(), PrimaryKeyColumns, PrimaryKeyColumns)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def forDeleteShouldRejectPartitionKeyColumnsAsDeleteColumns(): Unit = {
    TableWriter.validateDeleteSelectors(createTableDef(), PartitionKeyColumns, PrimaryKeyColumns)
  }

  @Test
  def deleteAllColumnsShouldIncludeRealColumnWhenPlaceholderNameMatchesRealColumn(): Unit = {
    val connector = createConnector()
    val ttlCol = ColumnDef("ttl_col", RegularColumn, IntType)
    val data = ColumnDef("data", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(ttlCol, data))
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_col"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(AllColumns)
    Assert.assertTrue("DELETE should include real column ttl_col even though it matches placeholder name",
      query.contains("\"ttl_col\""))
    Assert.assertTrue("DELETE should include data column",
      query.contains("\"data\""))
  }

  @Test
  def deleteSomeColumnsShouldFindRealColumnWhenPlaceholderNameMatchesRealColumn(): Unit = {
    val connector = createConnector()
    val ttlCol = ColumnDef("ttl_col", RegularColumn, IntType)
    val data = ColumnDef("data", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(ttlCol, data))
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_col"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    // Should not throw — ttl_col is a real column even though it matches the placeholder name
    val query = writer.deleteQueryTemplate(SomeColumns("ttl_col"))
    Assert.assertTrue("DELETE should reference ttl_col", query.contains("\"ttl_col\""))
  }

  @Test
  def insertQueryShouldKeepRealColumnWhenTtlPlaceholderNameMatchesColumn(): Unit = {
    val connector = createConnector()
    val ttlCol = ColumnDef("ttl_col", RegularColumn, IntType)
    val data = ColumnDef("data", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(ttlCol, data))
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_col"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, writeConf,
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingInsert
    Assert.assertTrue("INSERT should write the real ttl_col column",
      query.contains("\"ttl_col\""))
    Assert.assertTrue("INSERT should still use ttl_col as the per-row TTL placeholder",
      query.contains("USING TTL :ttl_col"))
  }

  @Test
  def insertQueryShouldNotWriteRealColumnWhenOnlyAutoSelectedAsTtlPlaceholder(): Unit = {
    val connector = createConnector()
    val ttlCol = ColumnDef("ttl_col", RegularColumn, IntType)
    val tableDef = createTableDef(regularColumns = Seq(ttlCol))
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_col"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf,
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingInsert
    Assert.assertFalse("INSERT should not write ttl_col unless selected as a table column",
      query.contains("\"ttl_col\""))
    Assert.assertTrue("INSERT should still bind ttl_col as the per-row TTL placeholder",
      query.contains("USING TTL :ttl_col"))
  }

  @Test
  def updateQueryShouldKeepRealColumnWhenTtlPlaceholderNameMatchesColumn(): Unit = {
    val connector = createConnector()
    val ttlCol = ColumnDef("ttl_col", RegularColumn, IntType)
    val setColumn = ColumnDef("scol", RegularColumn, SetType(TextType))
    val tableDef = createTableDef(regularColumns = Seq(ttlCol, setColumn))
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_col"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, SomeColumns("pk", "ck", "ttl_col", "scol".append), writeConf,
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertTrue("UPDATE should write the real ttl_col column",
      query.contains("\"ttl_col\" = :\"ttl_col\""))
    Assert.assertTrue("UPDATE should still use ttl_col as the per-row TTL placeholder",
      query.contains("USING TTL :ttl_col"))
  }

  @Test
  def staticColumnWriteWithPerRowTimestampShouldNotRequireClusteringKey(): Unit = {
    val connector = createConnector()
    val staticCol = ColumnDef("static_value", StaticColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(staticCol))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, SomeColumns("pk", "static_value"),
      WriteConf(timestamp = TimestampOption.perRow("write_ts")),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    Assert.assertEquals(Vector("pk", "static_value"), writer.columnNames)
    Assert.assertTrue("INSERT should use the per-row timestamp placeholder",
      writer.queryTemplateUsingInsert.contains("USING TIMESTAMP :write_ts"))
  }

  @Test
  def counterTableWithPerRowTtlShouldNotAutoSelectIgnoredTtlPlaceholder(): Unit = {
    val connector = createConnector()
    val counter = ColumnDef("c", RegularColumn, CounterType)
    val pk = ColumnDef("pk", PartitionKeyColumn, IntType)
    val tableDef = TableDef("test_ks", "test_counters", Seq(pk), Seq.empty, Seq(counter))
    val recordingFactory = new RecordingRowWriterFactory

    TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(ttl = TTLOption.perRow("ttl_col")),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)(recordingFactory)

    Assert.assertEquals(Vector("pk", "c"), recordingFactory.selectedColumns.map(_.columnName))
  }

  @Test
  def counterTableWithPerRowTimestampShouldNotAutoSelectIgnoredTimestampPlaceholder(): Unit = {
    val connector = createConnector()
    val counter = ColumnDef("c", RegularColumn, CounterType)
    val pk = ColumnDef("pk", PartitionKeyColumn, IntType)
    val tableDef = TableDef("test_ks", "test_counters", Seq(pk), Seq.empty, Seq(counter))
    val recordingFactory = new RecordingRowWriterFactory

    TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(timestamp = TimestampOption.perRow("ts_col")),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)(recordingFactory)

    Assert.assertEquals(Vector("pk", "c"), recordingFactory.selectedColumns.map(_.columnName))
  }

}
