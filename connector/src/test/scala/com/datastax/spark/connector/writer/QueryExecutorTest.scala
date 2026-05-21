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

import java.util.concurrent.CompletableFuture

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement}
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.junit.Test
import org.junit.Assert._
import org.mockito.Mockito._

import scala.concurrent.Await
import scala.concurrent.duration._

class QueryExecutorTest {

  @Test
  def queryExecutorShouldKeepLegacyConstructorSignature(): Unit = {
    val legacySignature = Seq(
      classOf[CqlSession],
      Integer.TYPE,
      classOf[Option[_]],
      classOf[Option[_]],
      Integer.TYPE).map(_.getName)

    val hasLegacySignature = classOf[QueryExecutor].getConstructors.exists { ctor =>
      ctor.getParameterTypes.map(_.getName).toSeq == legacySignature
    }

    assertTrue("QueryExecutor must keep the legacy 5-argument constructor for binary compatibility",
      hasLegacySignature)
  }

  @Test
  def executeAsyncShouldApplyWriteAttributesAtExecutionTime(): Unit = {
    val session = mock(classOf[CqlSession])
    val initialStatement = mock(classOf[BoundStatement])
    val idempotentStatement = mock(classOf[BoundStatement])
    val consistentStatement = mock(classOf[BoundStatement])
    val resultSet = mock(classOf[AsyncResultSet])

    when(initialStatement.setConsistencyLevel(DefaultConsistencyLevel.THREE))
      .thenReturn(idempotentStatement)
    when(idempotentStatement.setIdempotent(true))
      .thenReturn(consistentStatement)
    when(session.executeAsync(consistentStatement))
      .thenReturn(CompletableFuture.completedFuture(resultSet))

    val executor = new QueryExecutor(
      session,
      1,
      None,
      None,
      CassandraConnectorConf.QueryRetryMaxRetriesParam.default,
      Some(DefaultConsistencyLevel.THREE),
      Some(true))

    val future = executor.executeAsync(new RichBoundStatementWrapper(initialStatement))
    Await.result(future, 5.seconds)

    verify(initialStatement).setConsistencyLevel(DefaultConsistencyLevel.THREE)
    verify(idempotentStatement).setIdempotent(true)
    verify(session).executeAsync(consistentStatement)
    assertTrue(future.isCompleted)
  }
}
