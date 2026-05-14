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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{ColumnDefinition, ColumnDefinitions, PreparedStatement}
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.{ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.CqlWhereClause
import com.datastax.spark.connector.rdd.partitioner.CqlTokenRange
import com.datastax.spark.connector.rdd.partitioner.dht.{LongToken, TokenFactory, TokenRange}
import com.datastax.spark.connector.types.{IntType, TextType}
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

  private def tokenRange(start: LongToken, end: LongToken): CqlTokenRange[Long, LongToken] = {
    CqlTokenRange(TokenRange(start, end, Set.empty, tokenFactory))
  }

  private def preparedStatementWithVariables(count: Int): PreparedStatement = {
    val preparedStatement = mock[PreparedStatement]
    val definitions = variableDefinitions(count)
    when(preparedStatement.getVariableDefinitions).thenReturn(definitions)
    preparedStatement
  }

  private def variableDefinitions(count: Int): ColumnDefinitions = {
    val definitions = mock[ColumnDefinitions]
    val columns = (1 to count).map { _ =>
      val column = mock[ColumnDefinition]
      when(column.getType).thenReturn(DataTypes.BIGINT)
      column
    }
    when(definitions.iterator()).thenReturn(Arrays.asList(columns: _*).iterator())
    definitions
  }
}
