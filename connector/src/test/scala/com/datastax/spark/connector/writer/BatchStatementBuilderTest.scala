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

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType}
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito._

import scala.jdk.CollectionConverters._

class BatchStatementBuilderTest {

  private def richBoundStatement: RichBoundStatementWrapper = {
    val statement = mock(classOf[BoundStatement])
    when(statement.getConsistencyLevel).thenReturn(null)
    when(statement.getSerialConsistencyLevel).thenReturn(null)
    new RichBoundStatementWrapper(statement)
  }

  @Test
  def multiStatementBatchCarriesConsistencyAndIdempotenceOnBatch(): Unit = {
    val builder = new BatchStatementBuilder(
      DefaultBatchType.UNLOGGED,
      DefaultConsistencyLevel.QUORUM,
      isIdempotent = true)

    val richStatement = builder.maybeCreateBatch(Seq(richBoundStatement, richBoundStatement))
    assertTrue(richStatement.isInstanceOf[RichBatchStatementWrapper])

    val batch = richStatement.stmt.asInstanceOf[BatchStatement]
    assertEquals(DefaultConsistencyLevel.QUORUM, batch.getConsistencyLevel)
    assertEquals(java.lang.Boolean.TRUE, batch.isIdempotent)
    batch.asScala.foreach { child =>
      assertNull(child.getConsistencyLevel)
      assertNull(child.getSerialConsistencyLevel)
    }
  }

  @Test
  def singleStatementCarriesConsistencyAndIdempotenceOnBoundStatement(): Unit = {
    val initialStatement = mock(classOf[BoundStatement])
    val consistencyStatement = mock(classOf[BoundStatement])
    val updatedStatement = mock(classOf[BoundStatement])
    when(initialStatement.setConsistencyLevel(DefaultConsistencyLevel.QUORUM))
      .thenReturn(consistencyStatement)
    when(consistencyStatement.setIdempotent(true))
      .thenReturn(updatedStatement)
    val wrapper = new RichBoundStatementWrapper(initialStatement)
    val builder = new BatchStatementBuilder(
      DefaultBatchType.UNLOGGED,
      DefaultConsistencyLevel.QUORUM,
      isIdempotent = true)

    val richStatement = builder.maybeCreateBatch(Seq(wrapper))

    assertSame(wrapper, richStatement)
    assertSame(updatedStatement, wrapper.stmt)
    verify(initialStatement).setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
    verify(initialStatement, never()).setIdempotent(true)
    verify(consistencyStatement).setIdempotent(true)
  }
}
