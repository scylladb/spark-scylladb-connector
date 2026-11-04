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

package com.datastax.spark.connector.cql

import java.io.IOException
import java.net.{MalformedURLException, URL}
import java.nio.file.{Files, Paths}
import java.time.Duration

import com.datastax.bdp.spark.ContinuousPagingScanner
import com.datastax.dse.driver.api.core.DseProtocolVersion
import com.datastax.dse.driver.api.core.config.DseDriverOption
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption._
import com.datastax.oss.driver.api.core.config.{DriverConfigLoader, DriverExecutionProfile, ProgrammaticDriverConfigLoaderBuilder => PDCLB}
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.{ConfigParameter, DeprecatedConfigParameter, ReflectionUtil}
import org.apache.spark.{SparkConf, SparkEnv, SparkFiles}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Creates both native and Thrift connections to Cassandra.
  * The connector provides a DefaultConnectionFactory.
  * Other factories can be plugged in by setting `spark.cassandra.connection.factory` option. */
trait CassandraConnectionFactory extends Serializable {

  /** Creates and configures native Cassandra connection */
  def createSession(conf: CassandraConnectorConf): CqlSession

  /** List of allowed custom property names passed in SparkConf */
  def properties: Set[String] = Set.empty

  def getScanner(
                  readConf: ReadConf,
                  connConf: CassandraConnectorConf,
                  columnNames: IndexedSeq[String]): Scanner =
    new DefaultScanner(readConf, connConf, columnNames)

}

/** Performs no authentication. Use with `AllowAllAuthenticator` in Cassandra. */
object DefaultConnectionFactory extends CassandraConnectionFactory {
  @transient
  lazy private val logger = LoggerFactory.getLogger("com.datastax.spark.connector.cql.CassandraConnectionFactory")

  def connectorConfigBuilder(conf: CassandraConnectorConf, initBuilder: PDCLB) = {

    def basicProperties(builder: PDCLB): PDCLB = {
      val localCoreThreadCount = Math.max(1, Runtime.getRuntime.availableProcessors() - 1)
      builder
        .withInt(CONNECTION_POOL_LOCAL_SIZE, conf.localConnectionsPerExecutor.getOrElse(localCoreThreadCount)) // moved from CassandraConnector
        .withInt(CONNECTION_POOL_REMOTE_SIZE, conf.remoteConnectionsPerExecutor.getOrElse(1)) // moved from CassandraConnector
        .withInt(CONNECTION_INIT_QUERY_TIMEOUT, conf.connectTimeoutMillis)
        .withDuration(CONTROL_CONNECTION_TIMEOUT, Duration.ofMillis(conf.connectTimeoutMillis))
        .withDuration(METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofMillis(conf.connectTimeoutMillis))
        .withInt(REQUEST_TIMEOUT, conf.readTimeoutMillis)
        .withClass(RETRY_POLICY_CLASS, classOf[MultipleRetryPolicy])
        .withClass(RECONNECTION_POLICY_CLASS, classOf[ExponentialReconnectionPolicy])
        .withDuration(RECONNECTION_BASE_DELAY, Duration.ofMillis(conf.minReconnectionDelayMillis))
        .withDuration(RECONNECTION_MAX_DELAY, Duration.ofMillis(conf.maxReconnectionDelayMillis))
        .withInt(NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, conf.quietPeriodBeforeCloseMillis / 1000)
        .withInt(NETTY_ADMIN_SHUTDOWN_TIMEOUT, conf.timeoutBeforeCloseMillis / 1000)
        .withInt(NETTY_IO_SHUTDOWN_QUIET_PERIOD, conf.quietPeriodBeforeCloseMillis / 1000)
        .withInt(NETTY_IO_SHUTDOWN_TIMEOUT, conf.timeoutBeforeCloseMillis / 1000)
        .withBoolean(NETTY_DAEMON, true)
        .withBoolean(RESOLVE_CONTACT_POINTS, conf.resolveContactPoints)
        .withInt(MultipleRetryPolicy.MaxRetryCount, conf.queryRetryCount)
        .withDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, Duration.ofMillis(conf.readTimeoutMillis))
        .withDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofMillis(conf.readTimeoutMillis))
    }

    // compression option cannot be set to NONE (default)
    def compressionProperties(b: PDCLB): PDCLB =
      Option(conf.compression)
        .filter(_.toLowerCase != "none")
        .fold(b)(c => b.withString(PROTOCOL_COMPRESSION, c.toLowerCase))

    def localDCProperty(b: PDCLB): PDCLB =
      conf.localDC.map(b.withString(LOAD_BALANCING_LOCAL_DATACENTER, _)).getOrElse(b)

    // add ssl properties if ssl is enabled
    def ipBasedConnectionProperties(ipConf: IpBasedContactInfo) = (builder: PDCLB) => {
      builder
        .withStringList(CONTACT_POINTS, ipConf.hosts.map(h => s"${h.getHostString}:${h.getPort}").toList.asJava)
        .withClass(LOAD_BALANCING_POLICY_CLASS, classOf[LocalNodeFirstLoadBalancingPolicy])

      def clientAuthEnabled(value: Option[String]) =
        if (ipConf.cassandraSSLConf.clientAuthEnabled) value else None

      if (ipConf.cassandraSSLConf.enabled) {
        Seq(
          SSL_TRUSTSTORE_PATH -> ipConf.cassandraSSLConf.trustStorePath,
          SSL_TRUSTSTORE_PASSWORD -> ipConf.cassandraSSLConf.trustStorePassword,
          SSL_KEYSTORE_PATH -> clientAuthEnabled(ipConf.cassandraSSLConf.keyStorePath),
          SSL_KEYSTORE_PASSWORD -> clientAuthEnabled(ipConf.cassandraSSLConf.keyStorePassword))
          .foldLeft(builder) { case (b, (name, value)) =>
            value.map(b.withString(name, _)).getOrElse(b)
          }
          .withClass(SSL_ENGINE_FACTORY_CLASS, classOf[DefaultSslEngineFactory])
          .withStringList(SSL_CIPHER_SUITES, ipConf.cassandraSSLConf.enabledAlgorithms.toList.asJava)
          .withBoolean(SSL_HOSTNAME_VALIDATION, false) // TODO: this needs to be configurable by users. Set to false for our integration tests
      } else {
        builder
      }
    }

    // Advanced shard awareness opens each connection from a client source port that targets a shard. With client
    // routes it is opt-in, as in the driver's ClientRoutesConfig: off unless `shardAwareness` is set. ScyllaDB Cloud
    // private endpoints support it; a proxy that does not preserve the client source port, such as one without Proxy
    // Protocol v2, does not. Basic shard awareness, sending requests to a connection of the right shard, works either
    // way.
    // The driver ignores ClientRoutesConfig.withShardAwareness as of 4.19.4.0, so the option is set here, both ways:
    // the connector setting takes precedence over a value from application.conf or system properties.
    def clientRoutesProperties(b: PDCLB): PDCLB =
      conf.clientRoutes.fold(b)(routes =>
        b.withBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, routes.shardAwareness))

    val universalProperties: Seq[PDCLB => PDCLB] =
      Seq( basicProperties, compressionProperties, localDCProperty)

    val appliedProperties: Seq[PDCLB => PDCLB] = conf.contactInfo match {
      case ipConf: IpBasedContactInfo => universalProperties :+ ipBasedConnectionProperties(ipConf) :+ (clientRoutesProperties _)
      case other => universalProperties
    }

    appliedProperties.foldLeft(initBuilder){ case (builder, properties) => properties(builder)}
  }

  /** Creates and configures native Cassandra connection */
  override def createSession(conf: CassandraConnectorConf): CqlSession = {
    val configLoaderBuilder = DriverConfigLoader.programmaticBuilder()
    val configLoader = connectorConfigBuilder(conf, configLoaderBuilder).build()

    val initialBuilder = CqlSession.builder()

    val builderWithContactInfo =  conf.contactInfo match {
      case ipConf: IpBasedContactInfo =>
        val builder = ipConf.authConf.authProvider.fold(initialBuilder)(initialBuilder.withAuthProvider)
          .withConfigLoader(configLoader)
        conf.clientRoutes.fold(builder)(routes => builder.withClientRoutesConfig(routes.toDriver))
      case CloudBasedContactInfo(path, authConf) =>
        requireNoClientRoutes(conf)
        authConf.authProvider.fold(initialBuilder)(initialBuilder.withAuthProvider)
          .withCloudSecureConnectBundle(maybeGetLocalFile(path))
          .withConfigLoader(configLoader)
      case ProfileFileBasedContactInfo(path) =>
        requireNoClientRoutes(conf)
        //Ignore all programmatic config for now ... //todo maybe allow programmatic config here by changing the profile?
        logger.warn(s"Ignoring all programmatic configuration, only using configuration from $path")
        initialBuilder.withConfigLoader(DriverConfigLoader.fromUrl(maybeGetLocalFile(path)))
    }

    val appName = Option(SparkEnv.get).map(env => env.conf.getAppId).getOrElse("NoAppID")
    val session = builderWithContactInfo
      .withApplicationName(s"Spark-Cassandra-Connector-$appName")
      .withSchemaChangeListener(new MultiplexingSchemaListener())
      .build()

    try {
      conf.clientRoutes.filter(_.verify).foreach(verifyClientRoutes(session, conf, _))
    } catch {
      case NonFatal(e) =>
        session.close()
        throw e
    }
    session
  }

  private def verifyClientRoutes(session: CqlSession, conf: CassandraConnectorConf, routes: ClientRoutesConf): Unit =
    if (ClientRoutesVerifier.runsOnSparkDriver) {
      conf.localDC.orElse(policyDataCenter(session)) match {
        case Some(localDC) => ClientRoutesVerifier.verify(session, routes, localDC)
        case None => logger.warn("Client routes are not verified because the local datacenter is unknown.")
      }
    }

  /** The data center that [[LocalNodeFirstLoadBalancingPolicy]] chose, possibly inferred from the client routes. */
  private def policyDataCenter(session: CqlSession): Option[String] =
    session.getContext.getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME) match {
      case policy: LocalNodeFirstLoadBalancingPolicy => Option(policy.localDataCenter).filter(_.nonEmpty)
      case _ => None
    }

  /** Client routes are only supported with host/port contact points, see [[CassandraConnectorConf.fromSparkConf]]. */
  private def requireNoClientRoutes(conf: CassandraConnectorConf): Unit =
    if (conf.clientRoutes.isDefined)
      throw new IllegalArgumentException(
        s"Client routes require host/port contact points, they cannot be used with ${conf.contactInfo.endPointStr()}.")

  /**
    * Checks the Spark Temp work directory for the file in question, returning
    * it if exists, returning a generic URL from the string if not
    */
  def maybeGetLocalFile(path: String): URL = {
    val localPath = Paths.get(SparkFiles.get(path))
    if (Files.exists(localPath)) {
      logger.info(s"Found the $path locally at $localPath, using this local file.")
      localPath.toUri.toURL
    } else {
      try {
        new URL(path)
      } catch {
        case e: MalformedURLException =>
          throw new IOException(s"The provided path $path is not a valid URL nor an existing locally path. Provide an " +
            s"URL accessible to all executors or a path existing on all executors (you may use `spark.files` to " +
            s"distribute a file to each executor).", e)
      }
    }
  }

  def continuousPagingEnabled(session: CqlSession): Boolean = {
    val confEnabled = SparkEnv.get.conf.getBoolean(
      CassandraConnectionFactory.continuousPagingParam.name,
      CassandraConnectionFactory.continuousPagingParam.default)
    val pv = session.getContext.getProtocolVersion
    if (pv.getCode > DseProtocolVersion.DSE_V1.getCode && confEnabled) {
      logger.debug(s"Scan Method Being Set to Continuous Paging")
      true
    } else {
      logger.debug(s"Scan Mode Disabled or Connecting to Non-DSE Cassandra Cluster")
      false
    }
  }

  override def getScanner(
    readConf: ReadConf,
    connConf: CassandraConnectorConf,
    columnNames: scala.IndexedSeq[String]): Scanner = {

    val isContinuousPagingEnabled =
      new CassandraConnector(connConf).withSessionDo { continuousPagingEnabled }

    if (isContinuousPagingEnabled) {
      logger.debug("Using ContinousPagingScanner")
      ContinuousPagingScanner(readConf, connConf, columnNames)
    } else {
      logger.debug("Not Connected to DSE 5.1 or Greater Falling back to Non-Continuous Paging")
      new DefaultScanner(readConf, connConf, columnNames)
    }
  }
}

/** Entry point for obtaining `CassandraConnectionFactory` object from [[org.apache.spark.SparkConf SparkConf]],
  * used when establishing connections to Cassandra. */
object CassandraConnectionFactory {

  val ReferenceSection = CassandraConnectorConf.ReferenceSection
  """Name of a Scala module or class implementing
    |CassandraConnectionFactory providing connections to the Cassandra cluster""".stripMargin

  val FactoryParam = ConfigParameter[CassandraConnectionFactory](
    name = "spark.cassandra.connection.factory",
    section = ReferenceSection,
    default = DefaultConnectionFactory,
    description =
      """Name of a Scala module or class implementing
        |CassandraConnectionFactory providing connections to the Cassandra cluster""".stripMargin)

  val continuousPagingParam = ConfigParameter[Boolean] (
    name = "spark.dse.continuousPagingEnabled",
    section = "Continuous Paging",
    default = true,
    description =
      """Deprecated, has no effect: the ScyllaDB Java Driver 4.19.2.3 and newer only negotiates OSS protocol versions,
        |so scans never use DSE Continuous Paging. Still accepted, with a warning, so that configurations that set it
        |remain valid.""".stripMargin
  )

  val deprecatedContinuousPagingParam = DeprecatedConfigParameter (
    name = "spark.dse.continuous_paging_enabled",
    replacementParameter = Some(continuousPagingParam),
    deprecatedSince = "DSE 6.0.0"
  )

  def fromSparkConf(conf: SparkConf): CassandraConnectionFactory = {
    fromNameOrDefault(conf.getOption(FactoryParam.name))
  }

  def fromNameOrDefault(factoryName: Option[String]): CassandraConnectionFactory = {
    factoryName.map(ReflectionUtil.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(FactoryParam.default)
  }
}
