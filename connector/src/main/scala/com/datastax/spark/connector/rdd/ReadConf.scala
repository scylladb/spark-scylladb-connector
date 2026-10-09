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

import java.util.Locale

import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel, DriverTimeoutException}
import com.datastax.oss.driver.api.core.servererrors.OverloadedException
import com.datastax.spark.connector.util.{ConfigCheck, ConfigParameter, DeprecatedConfigParameter, Logging}
import com.datastax.spark.connector.writer.WriteConf.{ReferenceSection, ThroughputMiBPSParam}
import org.apache.spark.SparkConf

case class ConnectorReadRetryConf(
    maxRetries: Int = ConnectorReadRetryConf.DefaultMaxRetries,
    backoffType: String = ConnectorReadRetryConf.ExponentialBackoff,
    initialDelayMillis: Int = ConnectorReadRetryConf.DefaultInitialDelayMillis,
    maxDelayMillis: Int = ConnectorReadRetryConf.DefaultMaxDelayMillis,
    multiplier: Double = ConnectorReadRetryConf.DefaultMultiplier,
    jitterFactor: Double = ConnectorReadRetryConf.DefaultJitterFactor,
    retryOn: Set[String] = ConnectorReadRetryConf.DefaultRetryOn) extends Serializable {

  import ConnectorReadRetryConf._

  require(maxRetries >= 0, "connector read retry maxRetries must be greater than or equal to 0")
  require(initialDelayMillis >= 0, "connector read retry initialDelayMS must be greater than or equal to 0")
  require(maxDelayMillis >= initialDelayMillis,
    "connector read retry maxDelayMS must be greater than or equal to initialDelayMS")
  require(multiplier >= 1.0d, "connector read retry multiplier must be greater than or equal to 1.0")
  require(jitterFactor >= 0.0d && jitterFactor <= 1.0d,
    "connector read retry jitterFactor must be between 0.0 and 1.0")
  require(BackoffTypes.contains(normalizedBackoffType),
    s"connector read retry backoff.type must be one of ${BackoffTypes.mkString(", ")}")

  retryOn.foreach(validateThrowableClass)

  def isEnabled: Boolean =
    maxRetries > 0 && retryOn.nonEmpty

  def normalizedBackoffType: String =
    backoffType.trim.toLowerCase(Locale.ROOT)

  def retryOnClasses: Set[Class[_ <: Throwable]] =
    retryOn.flatMap(loadThrowableClasses)

  def delayMillis(attempt: Int): Long =
    delayMillis(attempt, java.util.concurrent.ThreadLocalRandom.current().nextDouble())

  private[connector] def delayMillis(attempt: Int, randomDouble: Double): Long = {
    require(attempt >= 1, "retry attempt must be greater than or equal to 1")
    require(randomDouble >= 0.0d && randomDouble <= 1.0d, "randomDouble must be between 0.0 and 1.0")

    val delayWithoutJitter = normalizedBackoffType match {
      case ConstBackoff => initialDelayMillis.toDouble
      case ExponentialBackoff =>
        initialDelayMillis.toDouble * math.pow(multiplier, attempt - 1)
    }
    val cappedDelay = math.min(delayWithoutJitter, maxDelayMillis.toDouble)
    val jitter = cappedDelay * jitterFactor
    val jitteredDelay = cappedDelay - jitter + (2.0d * jitter * randomDouble)
    math.min(maxDelayMillis.toDouble, math.max(0.0d, jitteredDelay)).round
  }
}

object ConnectorReadRetryConf {
  val ConstBackoff = "const"
  val ExponentialBackoff = "exponential"
  val BackoffTypes: Set[String] = Set(ConstBackoff, ExponentialBackoff)

  val DefaultMaxRetries = 3
  val DefaultInitialDelayMillis = 1000
  val DefaultMaxDelayMillis = 5000
  val DefaultMultiplier = 2.0d
  val DefaultJitterFactor = 0.2d
  val DefaultRetryOn: Set[String] = Set(
    classOf[DriverTimeoutException].getName,
    classOf[OverloadedException].getName)
  val DefaultRetryOnString: String = DefaultRetryOn.toSeq.sorted.mkString(",")

  // Keep this independent from class references so public config names survive driver API relocation.
  private val PublicDriverClassNamePrefix =
    Seq("com", "datastax", "oss", "driver").mkString(".") + "."
  private val DriverTimeoutExceptionClassNameSuffix = "api.core.DriverTimeoutException"
  private val RuntimeDriverClassNamePrefix = {
    val className = classOf[DriverTimeoutException].getName
    if (className.endsWith(DriverTimeoutExceptionClassNameSuffix)) {
      className.stripSuffix(DriverTimeoutExceptionClassNameSuffix)
    } else {
      PublicDriverClassNamePrefix
    }
  }

  def parseRetryOn(value: String): Set[String] =
    value.split(",").iterator.map(_.trim).filter(_.nonEmpty).toSet

  private[connector] def retryOnClassNameCandidates(className: String): Seq[String] = {
    val exact = Seq(className)
    val runtimeDriverClassName =
      if (className.startsWith(PublicDriverClassNamePrefix) &&
          RuntimeDriverClassNamePrefix != PublicDriverClassNamePrefix) {
        Seq(RuntimeDriverClassNamePrefix + className.stripPrefix(PublicDriverClassNamePrefix))
      } else {
        Seq.empty
      }

    (exact ++ runtimeDriverClassName).distinct
  }

  private[connector] def loadThrowableClasses(className: String): Set[Class[_ <: Throwable]] = {
    val loader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)
    val notFound = scala.collection.mutable.ArrayBuffer.empty[ClassNotFoundException]

    val classes = retryOnClassNameCandidates(className).flatMap { candidate =>
      try {
        Some(Class.forName(candidate, false, loader).asSubclass(classOf[Throwable]))
      } catch {
        case e: ClassNotFoundException =>
          notFound += e
          None
        case e: ClassCastException =>
          throw new IllegalArgumentException(s"connector read retry class is not a Throwable: $className", e)
      }
    }.toSet

    if (classes.isEmpty) {
      throw new IllegalArgumentException(
        s"connector read retry exception class not found: $className",
        notFound.headOption.orNull)
    }

    classes
  }

  private def validateThrowableClass(className: String): Unit =
    loadThrowableClasses(className)
}

/** Read settings for RDD
  *
  * @param splitCount number of partitions to divide the data into; unset by default
  * @param splitSizeInMB size of Cassandra data to be read in a single Spark task;
  *                      determines the number of partitions, but ignored if `splitCount` is set
  * @param fetchSizeInRows number of CQL rows to fetch in a single round-trip to Cassandra
  * @param consistencyLevel consistency level for reads, default LOCAL_ONE;
  *                         higher consistency level will disable data-locality
  * @param taskMetricsEnabled whether or not enable task metrics updates (requires Spark 1.2+)
  * @param readsPerSec maximum read throughput allowed per single core in requests/s while
  *                                  joining an RDD with C* table (joinWithCassandraTable operation)
  *                                  also used by enterprise integrations
  * @param connectorRetry connector-level read request retry settings */
case class ReadConf(
  splitCount: Option[Int] = None,
  splitSizeInMB: Int = ReadConf.SplitSizeInMBParam.default,
  fetchSizeInRows: Int = ReadConf.FetchSizeInRowsParam.default,
  consistencyLevel: ConsistencyLevel = ReadConf.ConsistencyLevelParam.default,
  taskMetricsEnabled: Boolean = ReadConf.TaskMetricParam.default,
  throughputMiBPS: Option[Double] = None,
  readsPerSec: Option[Int] = ReadConf.ReadsPerSecParam.default,
  parallelismLevel: Int = ReadConf.ParallelismLevelParam.default,
  executeAs: Option[String] = None,
  joinInClauseSize: Int = ReadConf.JoinInClauseSizeParam.default,
  connectorRetry: ConnectorReadRetryConf = ConnectorReadRetryConf())


object ReadConf extends Logging {
  val ReferenceSection = "Read Tuning Parameters"

  val ThroughputMiBPSParam = ConfigParameter[Option[Double]] (
    name = "spark.cassandra.input.throughputMBPerSec",
    section = ReferenceSection,
    default = None,
    description = """*(Floating points allowed)* <br> Maximum read throughput allowed
                    | per single core in MB/s. Effects point lookups as well as full
                    | scans.""".stripMargin)

  val SplitSizeInMBParam = ConfigParameter[Int](
    name = "spark.cassandra.input.split.sizeInMB",
    section = ReferenceSection,
    default = 512,
    description =
      """Approx amount of data to be fetched into a Spark partition. Minimum number of resulting Spark
        | partitions is <code>1 + 2 * SparkContext.defaultParallelism</code>
        |""".stripMargin.filter(_ >= ' '))

  val DeprecatedSplitSizeInMBParam = DeprecatedConfigParameter(
    name = "spark.cassandra.input.split.size_in_mb",
    replacementParameter = Some(SplitSizeInMBParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val FetchSizeInRowsParam = ConfigParameter[Int](
    name = "spark.cassandra.input.fetch.sizeInRows",
    section = ReferenceSection,
    default = 1000,
    description = """Number of CQL rows fetched per driver request""")

  val DeprecatedFetchSizeInRowsParam = DeprecatedConfigParameter(
    name = "spark.cassandra.input.fetch.size_in_rows",
    replacementParameter = Some(FetchSizeInRowsParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val ConsistencyLevelParam = ConfigParameter[ConsistencyLevel](
    name = "spark.cassandra.input.consistency.level",
    section = ReferenceSection,
    default = DefaultConsistencyLevel.LOCAL_ONE,
    description = """Consistency level to use when reading""")

  val TaskMetricParam = ConfigParameter[Boolean](
    name = "spark.cassandra.input.metrics",
    section = ReferenceSection,
    default = true,
    description = """Sets whether to record connector specific metrics on write"""
  )

  val ReadsPerSecParam = ConfigParameter[Option[Int]] (
    name = "spark.cassandra.input.readsPerSec",
    section = ReferenceSection,
    default = None,
    description =
      """Sets max requests or pages per core per second, unlimited by default."""
  )

  val DeprecatedReadsPerSecParam = DeprecatedConfigParameter(
    name = "spark.cassandra.input.reads_per_sec",
    replacementParameter = Some(ReadsPerSecParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val ThroughputJoinQueryPerSecParam = DeprecatedConfigParameter (
    name = "spark.cassandra.input.join.throughput_query_per_sec",
    replacementParameter = Some(ReadsPerSecParam),
    deprecatedSince = "DSE 5.1.5")


  val ParallelismLevelParam = ConfigParameter[Int] (
    name = "spark.cassandra.concurrent.reads",
    section = ReferenceSection,
    default = 512,
    description =
      """Sets read parallelism for joinWithCassandra tables"""
  )

  val ConnectorRetryMaxRetriesParam = ConfigParameter[Int] (
    name = "spark.cassandra.input.connectorRetry.maxRetries",
    section = ReferenceSection,
    default = ConnectorReadRetryConf.DefaultMaxRetries,
    description =
      "Maximum number of connector-level retries for failed read requests. " +
        "Set to 0 to disable connector-level read retries. This is separate from the Java driver retry policy " +
        "configured by spark.cassandra.query.retry.count."
  )

  val ConnectorRetryBackoffTypeParam = ConfigParameter[String] (
    name = "spark.cassandra.input.connectorRetry.backoff.type",
    section = ReferenceSection,
    default = ConnectorReadRetryConf.ExponentialBackoff,
    description =
      """Backoff type for connector-level read retries. Valid values are const and exponential."""
  )

  val ConnectorRetryInitialDelayParam = ConfigParameter[Int] (
    name = "spark.cassandra.input.connectorRetry.backoff.initialDelayMS",
    section = ReferenceSection,
    default = ConnectorReadRetryConf.DefaultInitialDelayMillis,
    description = """Initial delay in milliseconds for connector-level read retries."""
  )

  val ConnectorRetryMaxDelayParam = ConfigParameter[Int] (
    name = "spark.cassandra.input.connectorRetry.backoff.maxDelayMS",
    section = ReferenceSection,
    default = ConnectorReadRetryConf.DefaultMaxDelayMillis,
    description = """Maximum delay in milliseconds for connector-level read retries."""
  )

  val ConnectorRetryMultiplierParam = ConfigParameter[Double] (
    name = "spark.cassandra.input.connectorRetry.backoff.multiplier",
    section = ReferenceSection,
    default = ConnectorReadRetryConf.DefaultMultiplier,
    description = """Multiplier used by exponential connector-level read retry backoff."""
  )

  val ConnectorRetryJitterFactorParam = ConfigParameter[Double] (
    name = "spark.cassandra.input.connectorRetry.backoff.jitterFactor",
    section = ReferenceSection,
    default = ConnectorReadRetryConf.DefaultJitterFactor,
    description = """Jitter factor for connector-level read retry backoff, between 0.0 and 1.0."""
  )

  val ConnectorRetryOnParam = ConfigParameter[String] (
    name = "spark.cassandra.input.connectorRetry.on",
    section = ReferenceSection,
    default = ConnectorReadRetryConf.DefaultRetryOnString,
    description =
      "Comma-separated fully qualified exception class names retried by connector-level read retry. " +
        "Retries are attempted only for explicitly idempotent statements."
  )

  val JoinInClauseSizeParam = ConfigParameter[Int] (
    name = "spark.cassandra.input.join.inClauseSize",
    section = ReferenceSection,
    default = 0,
    description =
      """Controls IN-clause batching for joinWithCassandraTable operations.
        | Set to 0 to disable batching (default). When set to a value greater than 1,
        | consecutive left-side rows sharing the same partition key are grouped and
        | queried with a single SELECT ... WHERE pk = ? AND ck IN (?, ?, ...) statement,
        | reducing round-trips to the database. The value determines the maximum number
        | of clustering key values per IN clause. Only applies when the join includes
        | at least one clustering column.""".stripMargin.filter(_ >= ' ')
  )

  def fromSparkConf(conf: SparkConf): ReadConf = {

    ConfigCheck.checkConfig(conf)

    ReadConf(
      fetchSizeInRows = conf.getInt(FetchSizeInRowsParam.name, FetchSizeInRowsParam.default),
      splitSizeInMB = conf.getInt(SplitSizeInMBParam.name, SplitSizeInMBParam.default),
      consistencyLevel = DefaultConsistencyLevel.valueOf(
        conf.get(ConsistencyLevelParam.name, ConsistencyLevelParam.default.name)),
      taskMetricsEnabled = conf.getBoolean(TaskMetricParam.name, TaskMetricParam.default),
      throughputMiBPS = conf.getOption(ThroughputMiBPSParam.name).map(_.toDouble),
      readsPerSec = conf.getOption(ReadsPerSecParam.name).map(_.toInt),
      parallelismLevel = conf.getInt(ParallelismLevelParam.name, ParallelismLevelParam.default),
      joinInClauseSize = conf.getInt(JoinInClauseSizeParam.name, JoinInClauseSizeParam.default),
      connectorRetry = ConnectorReadRetryConf(
        maxRetries = conf.getInt(ConnectorRetryMaxRetriesParam.name, ConnectorRetryMaxRetriesParam.default),
        backoffType = conf.get(ConnectorRetryBackoffTypeParam.name, ConnectorRetryBackoffTypeParam.default),
        initialDelayMillis = conf.getInt(ConnectorRetryInitialDelayParam.name, ConnectorRetryInitialDelayParam.default),
        maxDelayMillis = conf.getInt(ConnectorRetryMaxDelayParam.name, ConnectorRetryMaxDelayParam.default),
        multiplier = conf.getDouble(ConnectorRetryMultiplierParam.name, ConnectorRetryMultiplierParam.default),
        jitterFactor = conf.getDouble(ConnectorRetryJitterFactorParam.name, ConnectorRetryJitterFactorParam.default),
        retryOn = ConnectorReadRetryConf.parseRetryOn(
          conf.get(ConnectorRetryOnParam.name, ConnectorRetryOnParam.default)))
    )
  }

}
