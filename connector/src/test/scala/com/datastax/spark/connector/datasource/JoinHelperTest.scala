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

import com.datastax.spark.connector.{ColumnName, SomeColumns}
import com.datastax.spark.connector.cql.{ClusteringColumn, ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.CqlWhereClause
import com.datastax.spark.connector.types.{IntType, TextType}
import org.junit.{Assert, Test}

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
}
