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

package com.datastax.spark.connector

import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.spark.connector.cql.{CassandraConnector, Schema, TableDef}

/** Useful stuff that didn't fit elsewhere. */
package object util {

  /** The ScyllaDB Java Driver removed DSE proxy execution, which `executeAs` relied on, in 4.19.2.3. */
  private[connector] val ExecuteAsUnsupported =
    "executeAs (DSE proxy execution) is not supported: the ScyllaDB Java Driver removed it in 4.19.2.3"

  /** ReadConf and WriteConf reject `executeAs` when they are created, so `proxyUser` is always empty here. */
  def maybeExecutingAs[StatementT <: Statement[StatementT]](stmt: StatementT, proxyUser: Option[String]): StatementT = {
    if (proxyUser.isDefined) throw new UnsupportedOperationException(ExecuteAsUnsupported)
    stmt
  }

  def schemaFromCassandra(
      connector: CassandraConnector,
      keyspaceName: Option[String] = None,
      tableName: Option[String] = None): Schema = {
    connector.withSessionDo(Schema.fromCassandra(_, keyspaceName, tableName))
  }

  def tableFromCassandra(
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String): TableDef = {
    connector.withSessionDo(Schema.tableFromCassandra(_, keyspaceName, tableName))
  }

}
