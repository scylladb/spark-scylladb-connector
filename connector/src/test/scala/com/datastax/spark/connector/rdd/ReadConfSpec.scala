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

package com.datastax.spark.connector.rdd

import com.datastax.oss.driver.api.core.DriverTimeoutException
import com.datastax.oss.driver.api.core.servererrors.OverloadedException
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

class ReadConfSpec extends FlatSpec with Matchers {

  "ReadConf" should "load connector read retry defaults" in {
    val retryConf = ReadConf.fromSparkConf(new SparkConf(false)).connectorRetry

    retryConf.maxRetries shouldBe 3
    retryConf.normalizedBackoffType shouldBe ConnectorReadRetryConf.ExponentialBackoff
    retryConf.initialDelayMillis shouldBe 1000
    retryConf.maxDelayMillis shouldBe 5000
    retryConf.multiplier shouldBe 2.0d
    retryConf.jitterFactor shouldBe 0.2d
    retryConf.retryOn should contain (classOf[DriverTimeoutException].getName)
    retryConf.retryOn should contain (classOf[OverloadedException].getName)
    retryConf.retryOnClasses should contain (classOf[DriverTimeoutException])
    retryConf.retryOnClasses should contain (classOf[OverloadedException])
  }

  it should "load connector read retry overrides" in {
    val conf = new SparkConf(false)
      .set(ReadConf.ConnectorRetryMaxRetriesParam.name, "7")
      .set(ReadConf.ConnectorRetryBackoffTypeParam.name, ConnectorReadRetryConf.ConstBackoff)
      .set(ReadConf.ConnectorRetryInitialDelayParam.name, "11")
      .set(ReadConf.ConnectorRetryMaxDelayParam.name, "13")
      .set(ReadConf.ConnectorRetryMultiplierParam.name, "1.5")
      .set(ReadConf.ConnectorRetryJitterFactorParam.name, "0.0")
      .set(ReadConf.ConnectorRetryOnParam.name, classOf[DriverTimeoutException].getName)

    val retryConf = ReadConf.fromSparkConf(conf).connectorRetry

    retryConf.maxRetries shouldBe 7
    retryConf.normalizedBackoffType shouldBe ConnectorReadRetryConf.ConstBackoff
    retryConf.initialDelayMillis shouldBe 11
    retryConf.maxDelayMillis shouldBe 13
    retryConf.multiplier shouldBe 1.5d
    retryConf.jitterFactor shouldBe 0.0d
    retryConf.retryOn shouldBe Set(classOf[DriverTimeoutException].getName)
  }

  it should "disable connector read retries with maxRetries zero" in {
    ConnectorReadRetryConf(maxRetries = 0).isEnabled shouldBe false
  }

  it should "disable connector read retries with an empty retry list" in {
    val conf = new SparkConf(false)
      .set(ReadConf.ConnectorRetryOnParam.name, "")

    ReadConf.fromSparkConf(conf).connectorRetry.isEnabled shouldBe false
  }

  "ConnectorReadRetryConf" should "calculate exponential backoff with a hard maximum delay" in {
    val conf = ConnectorReadRetryConf(
      initialDelayMillis = 100,
      maxDelayMillis = 350,
      multiplier = 2.0d,
      jitterFactor = 0.0d)

    conf.delayMillis(1, 0.5d) shouldBe 100L
    conf.delayMillis(2, 0.5d) shouldBe 200L
    conf.delayMillis(3, 0.5d) shouldBe 350L
  }

  it should "calculate constant backoff" in {
    val conf = ConnectorReadRetryConf(
      backoffType = ConnectorReadRetryConf.ConstBackoff,
      initialDelayMillis = 100,
      maxDelayMillis = 350,
      jitterFactor = 0.0d)

    conf.delayMillis(1, 0.5d) shouldBe 100L
    conf.delayMillis(3, 0.5d) shouldBe 100L
  }

  it should "apply jitter without exceeding maximum delay" in {
    val conf = ConnectorReadRetryConf(
      initialDelayMillis = 100,
      maxDelayMillis = 100,
      jitterFactor = 0.5d)

    conf.delayMillis(1, 0.0d) shouldBe 50L
    conf.delayMillis(1, 1.0d) shouldBe 100L
  }

  it should "reject unknown retry exception classes" in {
    intercept[IllegalArgumentException] {
      ConnectorReadRetryConf(retryOn = Set("not.a.RealException"))
    }.getMessage should include ("exception class not found")
  }

  it should "reject retry exception classes that are not throwables" in {
    intercept[IllegalArgumentException] {
      ConnectorReadRetryConf(retryOn = Set(classOf[String].getName))
    }.getMessage should include ("not a Throwable")
  }

  it should "resolve retry exception class names from driver runtime classes" in {
    ConnectorReadRetryConf.loadThrowableClasses(classOf[DriverTimeoutException].getName) shouldBe
      Set(classOf[DriverTimeoutException])
    ConnectorReadRetryConf.loadThrowableClasses(classOf[OverloadedException].getName) shouldBe
      Set(classOf[OverloadedException])
  }
}
