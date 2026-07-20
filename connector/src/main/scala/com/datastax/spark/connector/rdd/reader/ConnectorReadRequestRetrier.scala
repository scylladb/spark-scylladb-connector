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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, CompletionException, CompletionStage, ExecutionException, Executors, ScheduledExecutorService, TimeUnit}
import java.util.function.BiConsumer

import com.datastax.oss.driver.api.core.AllNodesFailedException
import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.spark.connector.rdd.ConnectorReadRetryConf
import com.datastax.spark.connector.util.Logging

import scala.jdk.CollectionConverters._

private[connector] class ConnectorReadRequestRetrier(conf: ConnectorReadRetryConf) extends Logging {

  import ConnectorReadRequestRetrier._

  def executeAsync[T](
      statement: Statement[_ <: Statement[_]],
      phase: String)(
      action: => CompletionStage[T]): CompletionStage[T] = {

    val result = new CompletableFuture[T]()
    val retryAttempts = new AtomicInteger(0)

    def tryOnce(): Unit = {
      if (!result.isDone) {
        val stage =
          try {
            action
          } catch {
            case t: Throwable => failedCompletionStage[T](t)
          }

        stage.whenComplete(new BiConsumer[T, Throwable] {
          override def accept(value: T, error: Throwable): Unit = {
            if (error == null) {
              result.complete(value)
            } else {
              val retryAttempt = retryAttempts.incrementAndGet()
              if (shouldRetry(statement, error, retryAttempt)) {
                val delayMs = conf.delayMillis(retryAttempt)
                logTrace(
                  s"Connector read request retry: phase=$phase " +
                    s"attempt=$retryAttempt/${conf.maxRetries} backoff=${delayMs}ms " +
                    s"error=${rootCause(error).getClass.getSimpleName}")
                RetryScheduler.schedule(new Runnable {
                  override def run(): Unit = tryOnce()
                }, delayMs, TimeUnit.MILLISECONDS)
              } else {
                logSkippedRetry(statement, phase, error, retryAttempt)
                result.completeExceptionally(error)
              }
            }
          }
        })
      }
    }

    tryOnce()
    result
  }

  private def shouldRetry(
      statement: Statement[_ <: Statement[_]],
      error: Throwable,
      retryAttempt: Int): Boolean =
    conf.isEnabled &&
      retryAttempt <= conf.maxRetries &&
      statement.isIdempotent == java.lang.Boolean.TRUE &&
      matchesConfiguredException(error, conf)

  private def logSkippedRetry(
      statement: Statement[_ <: Statement[_]],
      phase: String,
      error: Throwable,
      retryAttempt: Int): Unit = {
    if (isTraceEnabled() && matchesConfiguredException(error, conf)) {
      val reason =
        if (!conf.isEnabled) "disabled"
        else if (statement.isIdempotent != java.lang.Boolean.TRUE) "non-idempotent"
        else if (retryAttempt > conf.maxRetries) "exhausted"
        else "not-retryable"
      logTrace(
        s"Connector read request retry skipped: phase=$phase reason=$reason " +
          s"attempt=$retryAttempt/${conf.maxRetries} error=${rootCause(error).getClass.getSimpleName}")
    }
  }
}

private[reader] object ConnectorReadRequestRetrier {

  private val RetryScheduler: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1, (r: Runnable) => {
      val t = new Thread(r, "connector-read-request-retry-scheduler")
      t.setDaemon(true)
      t
    })

  private def failedCompletionStage[T](throwable: Throwable): CompletionStage[T] = {
    val future = new CompletableFuture[T]()
    future.completeExceptionally(throwable)
    future
  }

  private[reader] def rootCause(throwable: Throwable): Throwable = {
    def unwrap(t: Throwable): Throwable = t match {
      case e: CompletionException if e.getCause != null => unwrap(e.getCause)
      case e: ExecutionException if e.getCause != null => unwrap(e.getCause)
      case other => other
    }

    unwrap(throwable)
  }

  private[reader] def matchesConfiguredException(
      throwable: Throwable,
      conf: ConnectorReadRetryConf): Boolean = {

    val retryOnClasses = conf.retryOnClasses
    val seen = scala.collection.mutable.Set.empty[Throwable]

    def matches(t: Throwable): Boolean = {
      val error = rootCause(t)
      if (seen.contains(error)) {
        false
      } else {
        seen += error
        retryOnClasses.exists(_.isAssignableFrom(error.getClass)) ||
          Option(error.getCause).exists(matches) ||
          nestedAllNodesFailures(error).exists(matches)
      }
    }

    matches(throwable)
  }

  private def nestedAllNodesFailures(throwable: Throwable): Iterable[Throwable] = throwable match {
    case e: AllNodesFailedException =>
      e.getAllErrors.asScala.values.flatMap(_.asScala)
    case _ =>
      Iterable.empty
  }
}
