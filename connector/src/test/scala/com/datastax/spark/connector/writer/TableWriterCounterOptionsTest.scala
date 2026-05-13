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

import java.net.InetSocketAddress

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{CounterType, IntType}
import org.junit.{Assert, Test}

class TableWriterCounterOptionsTest {

  private def createConnector(): CassandraConnector = {
    val contactInfo = IpBasedContactInfo(Set(new InetSocketAddress("127.0.0.1", 9042)))
    val connectorConf = CassandraConnectorConf(contactInfo)
    new CassandraConnector(connectorConf)
  }

  private def counterTableDef: TableDef = {
    val pk = ColumnDef("pk", PartitionKeyColumn, IntType)
    val counter = ColumnDef("c", RegularColumn, CounterType)
    TableDef("test_ks", "test_counters", Seq(pk), Seq.empty, Seq(counter))
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
  def counterTableWithPerRowTtlShouldNotAutoSelectIgnoredTtlPlaceholder(): Unit = {
    val recordingFactory = new RecordingRowWriterFactory

    TableWriter[CassandraRow](
      createConnector(), counterTableDef, AllColumns, WriteConf(ttl = TTLOption.perRow("ttl_col")),
      checkPartitionKey = false, partitions = Array.empty, tokenRangeAcc = None)(recordingFactory)

    Assert.assertEquals(Vector("pk", "c"), recordingFactory.selectedColumns.map(_.columnName))
  }

  @Test
  def counterTableWithPerRowTimestampShouldNotAutoSelectIgnoredTimestampPlaceholder(): Unit = {
    val recordingFactory = new RecordingRowWriterFactory

    TableWriter[CassandraRow](
      createConnector(), counterTableDef, AllColumns, WriteConf(timestamp = TimestampOption.perRow("ts_col")),
      checkPartitionKey = false, partitions = Array.empty, tokenRangeAcc = None)(recordingFactory)

    Assert.assertEquals(Vector("pk", "c"), recordingFactory.selectedColumns.map(_.columnName))
  }
}
