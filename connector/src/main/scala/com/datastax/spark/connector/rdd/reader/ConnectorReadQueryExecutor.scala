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

package com.datastax.spark.connector.rdd.reader

import com.datastax.oss.driver.api.core.{AllNodesFailedException, CqlSession, NoNodeAvailableException, NodeUnavailableException}
import com.datastax.oss.driver.api.core.connection.BusyConnectionException
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.servererrors.OverloadedException
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.{ConnectorReadRetryConf, ReadConf}
import com.datastax.spark.connector.writer.{AsyncExecutor, RichStatement}

private[connector] object ConnectorReadQueryExecutor {

  private val AsyncExecutorRetryExceptionClasses = Set[Class[_ <: Throwable]](
    classOf[AllNodesFailedException],
    classOf[NoNodeAvailableException],
    classOf[NodeUnavailableException],
    classOf[BusyConnectionException],
    classOf[OverloadedException])

  def apply(
      session: CqlSession,
      readConf: ReadConf,
      connectorConf: CassandraConnectorConf): AsyncExecutor[RichStatement, AsyncResultSet] = {
    val retryConf = withoutAsyncExecutorOverlap(readConf.connectorRetry)
    val retrier = new ConnectorReadRequestRetrier(retryConf)

    new AsyncExecutor[RichStatement, AsyncResultSet](
      stmt => {
        val driverStatement = stmt.stmt
        retrier.executeAsync(driverStatement, "first-page") {
          session.executeAsync(driverStatement)
        }
      },
      readConf.parallelismLevel,
      None,
      None,
      maxRetries = connectorConf.queryRetryMaxRetries)
  }

  private def withoutAsyncExecutorOverlap(conf: ConnectorReadRetryConf): ConnectorReadRetryConf =
    conf.copy(retryOn = conf.retryOn.filterNot { className =>
      ConnectorReadRetryConf.loadThrowableClasses(className).exists(AsyncExecutorRetryExceptionClasses.contains)
    })
}
