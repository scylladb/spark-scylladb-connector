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

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Statement}
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.writer.AsyncExecutor.Handler

class QueryExecutor(
    session: CqlSession,
    maxConcurrentQueries: Int,
    successHandler: Option[Handler[RichStatement]],
    failureHandler: Option[Handler[RichStatement]],
    maxRetries: Int,
    consistencyLevel: Option[ConsistencyLevel],
    isIdempotent: Option[Boolean])

  extends AsyncExecutor[RichStatement, AsyncResultSet](
    stmt => session.executeAsync(QueryExecutor.applyWriteAttributes(stmt.stmt, consistencyLevel, isIdempotent)),
    maxConcurrentQueries,
    successHandler,
    failureHandler,
    maxRetries) {

  // Preserve the legacy JVM constructor signature for compiled callers.
  def this(
      session: CqlSession,
      maxConcurrentQueries: Int,
      successHandler: Option[Handler[RichStatement]],
      failureHandler: Option[Handler[RichStatement]],
      maxRetries: Int) =
    this(session, maxConcurrentQueries, successHandler, failureHandler, maxRetries, None, None)
}

object QueryExecutor {

  private[writer] def applyWriteAttributes(
      stmt: Statement[_ <: Statement[_]],
      consistencyLevel: Option[ConsistencyLevel],
      isIdempotent: Option[Boolean]): Statement[_ <: Statement[_]] = {
    val withConsistency = consistencyLevel match {
      case Some(level) => stmt.setConsistencyLevel(level)
      case None => stmt
    }
    isIdempotent match {
      case Some(flag) => withConsistency.setIdempotent(flag)
      case None => withConsistency
    }
  }

  def apply(
    session: CqlSession,
    maxConcurrentQueries: Int,
    successHandler: Option[Handler[RichStatement]],
    failureHandler: Option[Handler[RichStatement]]): QueryExecutor = {

    new QueryExecutor(
      session,
      maxConcurrentQueries,
      successHandler,
      failureHandler,
      CassandraConnectorConf.QueryRetryMaxRetriesParam.default)
  }

  def apply(
    session: CqlSession,
    maxConcurrentQueries: Int,
    successHandler: Option[Handler[RichStatement]],
    failureHandler: Option[Handler[RichStatement]],
    connectorConf: CassandraConnectorConf): QueryExecutor = {

    new QueryExecutor(
      session,
      maxConcurrentQueries,
      successHandler,
      failureHandler,
      maxRetries = connectorConf.queryRetryMaxRetries)
  }
}
