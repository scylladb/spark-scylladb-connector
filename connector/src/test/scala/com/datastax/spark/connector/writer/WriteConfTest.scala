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
import com.datastax.spark.connector.{BytesInBatch, PrimaryKeyColumns, RowsInBatch, SomeColumns}
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class WriteConfTest extends FlatSpec with Matchers {

  "WriteConf" should "be configured with proper defaults" in {
    val conf = new SparkConf(false)
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be (BytesInBatch(WriteConf.BatchSizeBytesParam.default))
    writeConf.consistencyLevel should be (WriteConf.ConsistencyLevelParam.default)
    writeConf.parallelismLevel should be (WriteConf.ParallelismLevelParam.default)
  }

  it should "allow setting the rate limit as a decimal" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.throughputMBPerSec", "0.5")
    val writeConf = WriteConf.fromSparkConf(conf)
      writeConf.throughputMiBPS.get should equal ( 0.5 +- 0.02 )
  }

  it should "allow to set consistency level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.consistency.level", "THREE")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.consistencyLevel should be(DefaultConsistencyLevel.THREE)
  }

  it should "allow to set parallelism level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.concurrent.writes", "17")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.parallelismLevel should be(17)
  }

  it should "allow to set batch size in bytes" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.bytes", "12345")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in bytes when rows are set to auto" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.bytes", "12345")
      .set("spark.cassandra.output.batch.size.rows", "auto")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(BytesInBatch(12345))
  }

  it should "allow to set batch size in rows" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.size.rows", "12345")
    val writeConf = WriteConf.fromSparkConf(conf)

    writeConf.batchSize should be(RowsInBatch(12345))
  }

  it should "allow to set batch level" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.grouping.key", "none")
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.batchGroupingKey should be(BatchGroupingKey.None)
  }

  it should "allow to set batch buffer size" in {
    val conf = new SparkConf(false)
      .set("spark.cassandra.output.batch.grouping.buffer.size", "30000")
    val writeConf = WriteConf.fromSparkConf(conf)
    writeConf.batchGroupingBufferSize should be(30000)
  }

  it should "keep TTL option placeholder in forDelete config for positional alignment" in {
    val conf = WriteConf(ttl = TTLOption.perRow("_ttl")).forDelete
    // TTL is kept in the delete config so option-placeholder columns remain available
    // for positional alignment in tuple-based RDDs. The DELETE template omits TTL.
    conf.optionPlaceholders should contain("_ttl")
  }

  it should "keep per-row TTL and timestamp in forDelete config" in {
    val wc = WriteConf(
      ttl = TTLOption.perRow("_ttl"),
      timestamp = TimestampOption.perRow("_ts"))
    val conf = wc.forDelete
    // TTL and timestamp are kept so that the RowWriter preserves tuple-positional mapping
    conf.ttl should be(TTLOption.perRow("_ttl"))
    conf.timestamp should be(TimestampOption.perRow("_ts"))
  }

  it should "strip ifNotExists and ignoreNulls in forDelete" in {
    val conf = WriteConf(ifNotExists = true, ignoreNulls = true).forDelete
    conf.ifNotExists should be(false)
    conf.ignoreNulls should be(false)
  }

  it should "strip static TTL in forDelete but preserve per-row TTL" in {
    val staticConf = WriteConf(ttl = TTLOption.constant(100)).forDelete
    staticConf.ttl should be(TTLOption.defaultValue)

    val perRowConf = WriteConf(ttl = TTLOption.perRow("_ttl")).forDelete
    perRowConf.ttl should be(TTLOption.perRow("_ttl"))
  }

  it should "reject duplicate per-row placeholder names for TTL and timestamp" in {
    val ex = intercept[IllegalArgumentException] {
      WriteConf(ttl = TTLOption.perRow("x"), timestamp = TimestampOption.perRow("x"))
    }
    ex.getMessage should include("different names")
  }

  it should "keep the legacy optionsAsColumns function accessor" in {
    val method = classOf[WriteConf].getMethod("optionsAsColumns")
    method.getReturnType.getName should be("scala.Function2")

    val optionsAsColumns = method.invoke(WriteConf(ttl = TTLOption.perRow("ttl_col")))
      .asInstanceOf[(String, String) => Seq[com.datastax.spark.connector.cql.ColumnDef]]

    optionsAsColumns("ks", "tbl").map(_.columnName) should contain("ttl_col")
  }

}
