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

import java.util.HashMap
import java.util.concurrent.{CompletableFuture, CompletionStage, ExecutionException, TimeUnit}

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement, ExecutionInfo, Row}
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.{AllNodesFailedException, DriverTimeoutException}
import com.datastax.oss.driver.api.core.servererrors.OverloadedException
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.{ConnectorReadRetryConf, ReadConf}
import com.datastax.spark.connector.writer.RichBoundStatementWrapper
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ConnectorReadRequestRetrierSpec extends FlatSpec with Matchers with MockitoSugar {

  private val retryConf = ConnectorReadRetryConf(
    initialDelayMillis = 0,
    maxDelayMillis = 0,
    jitterFactor = 0.0d,
    retryOn = Set(classOf[DriverTimeoutException].getName))

  "ConnectorReadRequestRetrier" should "retry configured failures for idempotent statements" in {
    val statement = idempotentStatement(true)
    val retrier = new ConnectorReadRequestRetrier(retryConf)
    var calls = 0

    val result = retrier.executeAsync(statement, "first-page") {
      calls += 1
      if (calls == 1) failedStage(new DriverTimeoutException("timeout"))
      else CompletableFuture.completedFuture("ok")
    }

    result.toCompletableFuture.get(5, TimeUnit.SECONDS) shouldBe "ok"
    calls shouldBe 2
  }

  it should "not retry non-idempotent statements" in {
    val statement = idempotentStatement(false)
    val retrier = new ConnectorReadRequestRetrier(retryConf)
    var calls = 0

    val result = retrier.executeAsync(statement, "first-page") {
      calls += 1
      failedStage[String](new DriverTimeoutException("timeout"))
    }

    intercept[ExecutionException] {
      result.toCompletableFuture.get(5, TimeUnit.SECONDS)
    }.getCause shouldBe a[DriverTimeoutException]
    calls shouldBe 1
  }

  it should "not retry statements without explicit idempotency" in {
    val statement = statementWithIdempotence(null)
    val retrier = new ConnectorReadRequestRetrier(retryConf)
    var calls = 0

    val result = retrier.executeAsync(statement, "first-page") {
      calls += 1
      failedStage[String](new DriverTimeoutException("timeout"))
    }

    intercept[ExecutionException] {
      result.toCompletableFuture.get(5, TimeUnit.SECONDS)
    }.getCause shouldBe a[DriverTimeoutException]
    calls shouldBe 1
  }

  it should "not retry when disabled" in {
    val statement = idempotentStatement(true)
    val retrier = new ConnectorReadRequestRetrier(retryConf.copy(maxRetries = 0))
    var calls = 0

    val result = retrier.executeAsync(statement, "first-page") {
      calls += 1
      failedStage[String](new DriverTimeoutException("timeout"))
    }

    intercept[ExecutionException] {
      result.toCompletableFuture.get(5, TimeUnit.SECONDS)
    }.getCause shouldBe a[DriverTimeoutException]
    calls shouldBe 1
  }

  it should "not retry exceptions missing from the configured list" in {
    val statement = idempotentStatement(true)
    val retrier = new ConnectorReadRequestRetrier(retryConf)
    var calls = 0
    val failure = new IllegalStateException("not transient")

    val result = retrier.executeAsync(statement, "first-page") {
      calls += 1
      failedStage[String](failure)
    }

    intercept[ExecutionException] {
      result.toCompletableFuture.get(5, TimeUnit.SECONDS)
    }.getCause shouldBe failure
    calls shouldBe 1
  }

  it should "match configured exception classes nested inside AllNodesFailedException" in {
    val node = mock[Node]
    val errors = new HashMap[Node, Throwable]()
    errors.put(node, new DriverTimeoutException("timeout"))
    val failure = AllNodesFailedException.fromErrors(errors)

    ConnectorReadRequestRetrier.matchesConfiguredException(failure, retryConf) shouldBe true
  }

  it should "match the default retry exception classes" in {
    val node = mock[Node]
    val conf = ConnectorReadRetryConf(initialDelayMillis = 0, maxDelayMillis = 0, jitterFactor = 0.0d)

    ConnectorReadRequestRetrier.matchesConfiguredException(
      new DriverTimeoutException("timeout"), conf) shouldBe true
    ConnectorReadRequestRetrier.matchesConfiguredException(
      new OverloadedException(node, "overloaded"), conf) shouldBe true
  }

  "PrefetchingResultSetIterator" should "retry failed next-page fetches" in {
    val statement = idempotentStatement(true)
    val row1 = mock[Row]
    val row2 = mock[Row]
    val firstPage = resultSet(statement, Seq(row1), hasMorePages = true)
    val secondPage = resultSet(statement, Seq(row2), hasMorePages = false)

    when(firstPage.fetchNextPage())
      .thenReturn(
        failedStage[AsyncResultSet](new DriverTimeoutException("timeout")),
        CompletableFuture.completedFuture(secondPage))

    val iterator = new PrefetchingResultSetIterator(firstPage, None, retryConf)

    iterator.next() shouldBe row1
    iterator.next() shouldBe row2
    iterator.hasNext shouldBe false
    verify(firstPage, times(2)).fetchNextPage()
  }

  "ConnectorReadQueryExecutor" should "retry first-page client timeouts before completing the task future" in {
    val session = mock[CqlSession]
    val connectorConf = mock[CassandraConnectorConf]
    val statement = idempotentStatement(true)
    val resultSet = mock[AsyncResultSet]
    val readConf = ReadConf(parallelismLevel = 1, connectorRetry = retryConf)

    when(connectorConf.queryRetryMaxRetries).thenReturn(0)
    val executor = ConnectorReadQueryExecutor(session, readConf, connectorConf)
    when(session.executeAsync(statement))
      .thenReturn(
        failedStage[AsyncResultSet](new DriverTimeoutException("timeout")),
        CompletableFuture.completedFuture(resultSet))

    Await.result(executor.executeAsync(new RichBoundStatementWrapper(statement)), 5.seconds) shouldBe resultSet
    verify(session, times(2)).executeAsync(statement)
  }

  private def idempotentStatement(value: Boolean): BoundStatement = {
    statementWithIdempotence(Boolean.box(value))
  }

  private def statementWithIdempotence(value: java.lang.Boolean): BoundStatement = {
    val statement = mock[BoundStatement]
    when(statement.isIdempotent).thenReturn(value)
    statement
  }

  private def resultSet(
      statement: BoundStatement,
      rows: Seq[Row],
      hasMorePages: Boolean): AsyncResultSet = {
    val resultSet = mock[AsyncResultSet]
    val executionInfo = mock[ExecutionInfo]
    doReturn(statement).when(executionInfo).getRequest
    when(resultSet.getExecutionInfo).thenReturn(executionInfo)
    when(resultSet.currentPage()).thenReturn(rows.asJava)
    when(resultSet.hasMorePages).thenReturn(hasMorePages)
    resultSet
  }

  private def failedStage[T](throwable: Throwable): CompletionStage[T] = {
    val future = new CompletableFuture[T]()
    future.completeExceptionally(throwable)
    future
  }
}
