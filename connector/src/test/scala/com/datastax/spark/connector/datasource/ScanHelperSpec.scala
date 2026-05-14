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

import java.io.IOException
import java.util.Arrays

import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.core.cql.{BoundStatement, ColumnDefinition, ColumnDefinitions, PreparedStatement}
import com.datastax.oss.driver.api.core.metadata.token.{Token => NativeToken}
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.spark.connector.{CassandraRowMetadata, SomeColumns}
import com.datastax.spark.connector.cql.{CassandraConnectionFactory, CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.cql.{ColumnDef, PartitionKeyColumn, RegularColumn, ScanResult, Scanner, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.{CqlWhereClause, ReadConf}
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CqlTokenRange}
import com.datastax.spark.connector.rdd.partitioner.dht.{LongToken, TokenFactory, TokenRange}
import com.datastax.spark.connector.types.{IntType, TextType}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.mockito.ArgumentMatchers.{any => anyArg, anyInt}
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class ScanHelperSpec extends FlatSpec with Matchers with MockitoSugar {

  private implicit val tokenFactory: TokenFactory[Long, LongToken] = TokenFactory.Murmur3TokenFactory

  private val tableDef = TableDef(
    "test_ks",
    "test_table",
    Seq(ColumnDef("id", PartitionKeyColumn, IntType)),
    Seq.empty,
    Seq(ColumnDef("value", RegularColumn, TextType)))

  private val queryParts = CqlQueryParts(
    selectedColumnRefs = SomeColumns("id", "value").selectFrom(tableDef),
    whereClause = CqlWhereClause.empty)

  private val schema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("value", StringType, nullable = true)))

  "ScanHelper" should "cache prepared statements by exact token range CQL template" in {
    val (oneBindCql, oneBindValues) =
      ScanHelper.tokenRangeToCqlQuery(tokenRange(tokenFactory.minToken, tokenFactory.minToken), tableDef, queryParts)
    val (twoBindCql, twoBindValues) =
      ScanHelper.tokenRangeToCqlQuery(tokenRange(LongToken(0L), LongToken(10L)), tableDef, queryParts)
    val session = mock[CqlSession]
    val oneBindStatement = mock[PreparedStatement]
    val twoBindStatement = mock[PreparedStatement]
    val cache = ScanHelper.newPreparedStatementCache()

    oneBindValues should have size 1
    twoBindValues should have size 2
    oneBindCql should not equal twoBindCql
    when(session.prepare(oneBindCql)).thenReturn(oneBindStatement)
    when(session.prepare(twoBindCql)).thenReturn(twoBindStatement)

    ScanHelper.getOrPrepareScanStatement(session, oneBindCql, cache) shouldBe oneBindStatement
    ScanHelper.getOrPrepareScanStatement(session, twoBindCql, cache) shouldBe twoBindStatement
    ScanHelper.getOrPrepareScanStatement(session, oneBindCql, cache) shouldBe oneBindStatement

    verify(session, times(1)).prepare(oneBindCql)
    verify(session, times(1)).prepare(twoBindCql)
  }

  it should "scan mixed token range CQL templates through the partition reader cache" in {
    val oneBindRange = tokenRange(tokenFactory.minToken, tokenFactory.minToken)
    val twoBindRange = tokenRange(LongToken(0L), LongToken(10L))
    val (oneBindCql, _) = ScanHelper.tokenRangeToCqlQuery(oneBindRange, tableDef, queryParts)
    val (twoBindCql, _) = ScanHelper.tokenRangeToCqlQuery(twoBindRange, tableDef, queryParts)
    val connector = mock[CassandraConnector]
    val connectionFactory = mock[CassandraConnectionFactory]
    val connectorConf = mock[CassandraConnectorConf]
    val scanner = mock[Scanner]
    val session = mock[CqlSession]
    val metadata = CassandraRowMetadata(IndexedSeq("id", "value"))
    val oneBindBoundStatement = boundStatement()
    val twoBindBoundStatement = boundStatement()
    val oneBindStatement = preparedStatementWithVariables(1, oneBindBoundStatement)
    val twoBindStatement = preparedStatementWithVariables(2, twoBindBoundStatement)
    val readConf = ReadConf()
    val partition = CassandraPartition(0, Array.empty[String], Seq(oneBindRange, twoBindRange), 0L)
    val readerFactory = CassandraScanPartitionReaderFactory(connector, tableDef, schema, readConf, queryParts)

    when(connector.connectionFactory).thenReturn(connectionFactory)
    when(connector.conf).thenReturn(connectorConf)
    when(connectionFactory.getScanner(readConf, connectorConf, IndexedSeq("id", "value")))
      .thenReturn(scanner)
    when(scanner.getSession()).thenReturn(session)
    when(session.prepare(oneBindCql)).thenReturn(oneBindStatement)
    when(session.prepare(twoBindCql)).thenReturn(twoBindStatement)
    when(scanner.scan(oneBindBoundStatement)).thenReturn(ScanResult(Iterator.empty, metadata))
    when(scanner.scan(twoBindBoundStatement)).thenReturn(ScanResult(Iterator.empty, metadata))

    val reader = readerFactory.createReader(partition)

    reader.next() shouldBe false

    verify(session, times(1)).prepare(oneBindCql)
    verify(session, times(1)).prepare(twoBindCql)
    verify(oneBindStatement, times(1)).bind(anyArg[Object]())
    verify(twoBindStatement, times(1)).bind(anyArg[Object](), anyArg[Object]())
    verify(scanner, times(1)).scan(oneBindBoundStatement)
    verify(scanner, times(1)).scan(twoBindBoundStatement)
    reader.close()
  }

  it should "reject bind value count mismatches before binding" in {
    val tooFew = intercept[IOException] {
      ScanHelper.bindScanStatement(preparedStatementWithVariables(2), 1L)
    }
    val tooMany = intercept[IOException] {
      ScanHelper.bindScanStatement(preparedStatementWithVariables(1), 1L, 2L)
    }

    tooFew.getMessage should include ("expected 2 bind values, but got 1")
    tooMany.getMessage should include ("expected 1 bind values, but got 2")
  }

  it should "wrap variable metadata failures while binding prepared scan statements" in {
    val preparedStatement = mock[PreparedStatement]
    val failure = new IllegalStateException("variable metadata unavailable")
    when(preparedStatement.getVariableDefinitions).thenThrow(failure)

    val thrown = intercept[IOException] {
      ScanHelper.bindScanStatement(preparedStatement, 1L)
    }

    thrown.getMessage should include ("Exception during binding of prepared statement")
    thrown.getCause shouldBe failure
  }

  private def tokenRange(start: LongToken, end: LongToken): CqlTokenRange[Long, LongToken] = {
    CqlTokenRange(TokenRange(start, end, Set.empty, tokenFactory))
  }

  private def preparedStatementWithVariables(count: Int): PreparedStatement =
    preparedStatementWithVariables(count, boundStatement())

  private def preparedStatementWithVariables(count: Int, boundStatement: BoundStatement): PreparedStatement = {
    val preparedStatement = mock[PreparedStatement]
    val definitions = variableDefinitions(count)
    when(preparedStatement.getVariableDefinitions).thenReturn(definitions)
    count match {
      case 0 => when(preparedStatement.bind()).thenReturn(boundStatement)
      case 1 => when(preparedStatement.bind(anyArg[Object]())).thenReturn(boundStatement)
      case 2 => when(preparedStatement.bind(anyArg[Object](), anyArg[Object]())).thenReturn(boundStatement)
      case _ => throw new IllegalArgumentException(s"Unsupported test variable count: $count")
    }
    preparedStatement
  }

  private def boundStatement(): BoundStatement = {
    val statement = mock[BoundStatement]
    when(statement.setIdempotent(true)).thenReturn(statement)
    when(statement.setConsistencyLevel(anyArg[ConsistencyLevel]())).thenReturn(statement)
    when(statement.setPageSize(anyInt())).thenReturn(statement)
    when(statement.setRoutingToken(anyArg[NativeToken]())).thenReturn(statement)
    statement
  }

  private def variableDefinitions(count: Int): ColumnDefinitions = {
    val definitions = mock[ColumnDefinitions]
    val columns = (1 to count).map { _ =>
      val column = mock[ColumnDefinition]
      when(column.getType).thenReturn(DataTypes.BIGINT)
      column
    }
    when(definitions.iterator()).thenAnswer(_ => Arrays.asList(columns: _*).iterator())
    definitions
  }
}
