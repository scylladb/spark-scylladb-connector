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

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.config.DefaultDriverOption.{CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, LOAD_BALANCING_LOCAL_DATACENTER}
import com.datastax.oss.driver.api.core.config.{DriverConfigLoader, DriverExecutionProfile}
import com.datastax.spark.connector.cql.CassandraConnectorConf._
import com.datastax.spark.connector.datasource.CassandraSourceUtil
import com.datastax.spark.connector.util.ConfigCheck
import com.datastax.spark.connector.util.ConfigCheck.ConnectorConfigurationException
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

import scala.jdk.CollectionConverters._

class ClientRoutesConfSpec extends FlatSpec with Matchers {

  private val EndpointsKey = ClientRoutesEndpointsParam.name

  private def sparkConf(settings: (String, String)*): SparkConf =
    new SparkConf(loadDefaults = false)
      .set(ConnectionHostParam.name, "127.0.0.1")
      .set(LocalDCParam.name, "dc1")
      .setAll(settings)

  private def routesOf(settings: (String, String)*): Option[ClientRoutesConf] =
    CassandraConnectorConf.fromSparkConf(sparkConf(settings: _*)).clientRoutes

  private def parseError(endpoints: String): String =
    intercept[IllegalArgumentException](routesOf(EndpointsKey -> endpoints)).getMessage

  private val contactPoint = IpBasedContactInfo(Set(new InetSocketAddress("127.0.0.1", 9042)))
  private val routes = ClientRoutesConf(Seq(ClientRouteEndpointConf("id-dc1")))

  "Client routes SparkConf parsing" should "leave client routes disabled by default" in {
    routesOf() shouldBe None
  }

  it should "parse a single connection ID" in {
    routesOf(EndpointsKey -> "id-dc1") shouldBe Some(ClientRoutesConf(Seq(ClientRouteEndpointConf("id-dc1"))))
  }

  it should "parse several connection IDs and address overrides" in {
    routesOf(EndpointsKey -> "id-dc1,id-dc2=vpce.internal,id-dc3=10.0.0.5") shouldBe Some(ClientRoutesConf(Seq(
      ClientRouteEndpointConf("id-dc1"),
      ClientRouteEndpointConf("id-dc2", Some("vpce.internal")),
      ClientRouteEndpointConf("id-dc3", Some("10.0.0.5")))))
  }

  it should "keep every connection ID in order, e.g. one per availability zone of several datacenters" in {
    val ids = for (dc <- 1 to 2; az <- 1 to 3) yield s"id-dc$dc-az$az"

    val routes = routesOf(EndpointsKey -> ids.mkString(",")).get

    routes.connectionIds shouldBe ids
    routes.toDriver.getEndpoints.asScala.map(_.getConnectionId) shouldBe ids
  }

  it should "trim whitespace around entries, IDs and addresses" in {
    routesOf(EndpointsKey -> " id-dc1 , id-dc2 = vpce.internal ") shouldBe Some(ClientRoutesConf(Seq(
      ClientRouteEndpointConf("id-dc1"),
      ClientRouteEndpointConf("id-dc2", Some("vpce.internal")))))
  }

  it should "accept an IPv6 address override" in {
    routesOf(EndpointsKey -> "id-dc1=::1").get.endpoints shouldBe Seq(ClientRouteEndpointConf("id-dc1", Some("::1")))
  }

  it should "read shard awareness, disabled by default" in {
    routesOf(EndpointsKey -> "id-dc1").get.shardAwareness shouldBe false
    routesOf(EndpointsKey -> "id-dc1", ClientRoutesShardAwarenessParam.name -> "true").get.shardAwareness shouldBe true
  }

  it should "ignore shard awareness when no connection ID is set" in {
    routesOf(ClientRoutesShardAwarenessParam.name -> "true") shouldBe None
  }

  it should "reject an empty list" in {
    val error = parseError(" ")
    error should include(EndpointsKey)
    error should include("at least one connection ID is required")
  }

  it should "reject an empty entry" in {
    val error = parseError("id-dc1,,id-dc2")
    error should include(EndpointsKey)
    error should include("entry 2 is empty")
    parseError("id-dc1,") should include("entry 2 is empty")
  }

  it should "reject an empty connection ID" in {
    val error = parseError("=vpce.internal")
    error should include(EndpointsKey)
    error should include("entry 1: the connection ID must not be empty")
  }

  it should "reject an empty address override" in {
    val error = parseError("id-dc1, id-dc2=")
    error should include(EndpointsKey)
    error should include("entry 2: the connection address must not be empty when set")
  }

  it should "reject duplicate connection IDs" in {
    val error = parseError("id-dc1,id-dc2,id-dc1")
    error should include(EndpointsKey)
    error should include("duplicate connection ID(s): id-dc1")
  }

  it should "reject an address override with a port" in {
    val error = parseError("id-dc1=vpce.internal:9042")
    error should include(EndpointsKey)
    error should include("without a port")
  }

  it should "reject an address override with invalid characters" in {
    parseError("id-dc1=vpce internal") should include("invalid characters")
    parseError("id-dc1=a=b") should include("invalid characters")
  }

  it should "report every problem at once" in {
    parseError("id-dc1,,id-dc1") should (include("entry 2 is empty") and include("duplicate connection ID(s): id-dc1"))
    parseError("id-dc1,id-dc1=") should (include("entry 2: the connection address") and include("duplicate"))
    parseError("=vpce.internal:9042,") should (include("entry 1: the connection ID must not be empty") and
      include("without a port") and include("entry 2 is empty"))
  }

  it should "read the native transport port, 9042 by default" in {
    routesOf(EndpointsKey -> "id-dc1").get.nativeTransportPort shouldBe 9042
    routesOf(EndpointsKey -> "id-dc1", ClientRoutesNativeTransportPortParam.name -> " 9142 ").get
      .nativeTransportPort shouldBe 9142
  }

  it should "reject an invalid native transport port" in {
    for (port <- Seq("0", "65536", "-1", "9042x")) {
      val error = intercept[IllegalArgumentException](
        routesOf(EndpointsKey -> "id-dc1", ClientRoutesNativeTransportPortParam.name -> port)).getMessage
      error should include(ClientRoutesNativeTransportPortParam.name)
      error should include("between 1 and 65535")
    }
  }

  "Client routes validation" should "reject client routes combined with a cloud bundle" in {
    val error = intercept[IllegalArgumentException](
      routesOf(EndpointsKey -> "id-dc1", CloudBasedConfigurationParam.name -> "bundle.zip")).getMessage
    error should include(EndpointsKey)
    error should include(CloudBasedConfigurationParam.name)
  }

  it should "reject client routes combined with a driver profile file" in {
    val error = intercept[IllegalArgumentException](
      routesOf(EndpointsKey -> "id-dc1", ProfileFileBasedConfigurationParam.name -> "driver.conf")).getMessage
    error should include(EndpointsKey)
    error should include(ProfileFileBasedConfigurationParam.name)
  }

  it should "not require the local DC, which can be inferred from the routes" in {
    val conf = CassandraConnectorConf.fromSparkConf(sparkConf(EndpointsKey -> "id-dc1").remove(LocalDCParam.name))
    conf.localDC shouldBe None
    conf.clientRoutes shouldBe Some(routes)
  }

  it should "reject invalid endpoints built programmatically" in {
    intercept[IllegalArgumentException](ClientRoutesConf(Nil)).getMessage should
      include("at least one connection ID is required")
    intercept[IllegalArgumentException](
      ClientRoutesConf(Seq(ClientRouteEndpointConf("a"), ClientRouteEndpointConf("a")))).getMessage should
      include("duplicate connection ID(s): a")
    intercept[IllegalArgumentException](
      ClientRoutesConf(Seq(ClientRouteEndpointConf("a", Some("host:9042"))))).getMessage should
      include("without a port")
    intercept[IllegalArgumentException](
      ClientRoutesConf(Seq(ClientRouteEndpointConf("a")), nativeTransportPort = 0)).getMessage should
      include("the native transport port must be between 1 and 65535, got 0")
  }

  it should "report both an empty connection ID and an empty address of the same endpoint" in {
    def error(endpoint: ClientRouteEndpointConf): String =
      intercept[IllegalArgumentException](ClientRoutesConf(Seq(endpoint))).getMessage

    error(ClientRouteEndpointConf("", Some(""))) should (include("endpoint 1: the connection ID must not be empty") and
      include("endpoint 1: the connection address must not be empty when set"))
    error(ClientRouteEndpointConf(" ", Some("host:1"))) should
      (include("endpoint 1: the connection ID must not be empty") and include("without a port"))
  }

  it should "refuse to open a session with a cloud bundle or a profile file" in {
    for (contactInfo <- Seq(CloudBasedContactInfo("bundle.zip", NoAuthConf), ProfileFileBasedContactInfo("driver.conf"))) {
      val conf = CassandraConnectorConf(contactInfo, clientRoutes = Some(routes))
      intercept[IllegalArgumentException](DefaultConnectionFactory.createSession(conf)).getMessage should
        include("Client routes require host/port contact points")
    }
  }

  "Client routes configuration keys" should "be accepted by ConfigCheck" in {
    ConfigCheck.checkConfig(new SparkConf(loadDefaults = false)
      .set(EndpointsKey, "id-dc1")
      .set(ClientRoutesShardAwarenessParam.name, "false")
      .set(ClientRoutesNativeTransportPortParam.name, "9042"))
  }

  it should "be suggested for misspelled keys" in {
    val misspelled = "spark.cassandra.connection.clientRoutes.endpoint"
    val error = the[ConnectorConfigurationException] thrownBy
      ConfigCheck.checkConfig(new SparkConf(loadDefaults = false).set(misspelled, "id-dc1"))
    error.getMessage should include(misspelled)
    error.getMessage should include(EndpointsKey)
  }

  it should "be consolidated from cluster level settings" in {
    val sqlConf = Map(
      s"cluster1/$EndpointsKey" -> "id-dc1=vpce.internal",
      s"cluster1/${ClientRoutesShardAwarenessParam.name}" -> "true",
      s"cluster2/$EndpointsKey" -> "id-other")
    val consolidated = CassandraSourceUtil.consolidateConfs(sparkConf(), sqlConf, cluster = "cluster1")
    CassandraConnectorConf.fromSparkConf(consolidated).clientRoutes shouldBe
      Some(ClientRoutesConf(Seq(ClientRouteEndpointConf("id-dc1", Some("vpce.internal"))), shardAwareness = true))
  }

  "CassandraConnectorConf with client routes" should "not be equal to a conf without them" in {
    val plain = CassandraConnectorConf(contactPoint, localDC = Some("dc1"))
    val routed = plain.copy(clientRoutes = Some(routes))
    routed shouldNot equal(plain)
    routed.hashCode shouldNot equal(plain.hashCode)
  }

  it should "not be equal to a conf with other routes" in {
    val routed = CassandraConnectorConf(contactPoint, clientRoutes = Some(routes))
    val otherIds = routed.copy(clientRoutes = Some(ClientRoutesConf(Seq(ClientRouteEndpointConf("id-dc2")))))
    val otherAddr = routed.copy(clientRoutes = Some(ClientRoutesConf(Seq(ClientRouteEndpointConf("id-dc1", Some("a"))))))
    val shardAware = routed.copy(clientRoutes = Some(routes.copy(shardAwareness = true)))
    val otherPort = routed.copy(clientRoutes = Some(routes.copy(nativeTransportPort = 9142)))
    Seq(otherIds, otherAddr, shardAware, otherPort).foreach(_ shouldNot equal(routed))
  }

  it should "be equal to a conf with the same routes" in {
    val conf1 = CassandraConnectorConf.fromSparkConf(sparkConf(EndpointsKey -> "id-dc1,id-dc2=vpce.internal"))
    val conf2 = CassandraConnectorConf.fromSparkConf(sparkConf(EndpointsKey -> "id-dc1, id-dc2 = vpce.internal"))
    conf1 shouldEqual conf2
    conf1.hashCode shouldEqual conf2.hashCode
  }

  it should "survive Java serialization" in {
    val conf = CassandraConnectorConf.fromSparkConf(
      sparkConf(EndpointsKey -> "id-dc1,id-dc2=vpce.internal", ClientRoutesShardAwarenessParam.name -> "true"))
    val copy = SerializationUtils.roundtrip(conf)
    copy shouldEqual conf
    copy.clientRoutes shouldEqual conf.clientRoutes
  }

  "ClientRoutesConf.toDriver" should "map every endpoint, the shard awareness flag and the native transport port" in {
    val driverConf = ClientRoutesConf(
      Seq(ClientRouteEndpointConf("id-dc1"), ClientRouteEndpointConf("id-dc2", Some("vpce.internal"))),
      shardAwareness = true,
      nativeTransportPort = 9142).toDriver

    driverConf.getEndpoints.asScala.map(e => (e.getConnectionId, Option(e.getConnectionAddrOverride))) shouldBe
      Seq(("id-dc1", None), ("id-dc2", Some("vpce.internal")))
    driverConf.isShardAwarenessEnabled shouldBe true
    driverConf.getNativeTransportPort shouldBe 9142
  }

  it should "disable shard awareness and use the native transport port 9042 by default" in {
    routes.toDriver.isShardAwarenessEnabled shouldBe false
    routes.toDriver.getNativeTransportPort shouldBe 9042
  }

  "DefaultConnectionFactory with client routes" should "turn advanced shard awareness off unless shardAwareness is set" in {
    def driverProfile(
      clientRoutes: Option[ClientRoutesConf],
      advancedShardAwareness: Option[Boolean] = None): DriverExecutionProfile = {
      val conf = CassandraConnectorConf(contactPoint, localDC = Some("dc1"), clientRoutes = clientRoutes)
      // A value set before the connector's own settings, as application.conf or a system property would
      val initial = advancedShardAwareness.foldLeft(DriverConfigLoader.programmaticBuilder())(
        _.withBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, _))
      val loader = DefaultConnectionFactory.connectorConfigBuilder(conf, initial).build()
      try loader.getInitialConfig.getDefaultProfile finally loader.close()
    }
    val driverDefault = {
      val loader = DriverConfigLoader.programmaticBuilder().build()
      try loader.getInitialConfig.getDefaultProfile.getBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED)
      finally loader.close()
    }

    val routed = driverProfile(Some(routes))
    routed.getBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED) shouldBe false
    routed.getString(LOAD_BALANCING_LOCAL_DATACENTER) shouldBe "dc1" // the connector's own settings still apply

    driverProfile(Some(routes.copy(shardAwareness = true)))
      .getBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED) shouldBe driverDefault
    driverProfile(None).getBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED) shouldBe driverDefault
    driverDefault shouldBe true

    driverProfile(Some(routes.copy(shardAwareness = true)), advancedShardAwareness = Some(false))
      .getBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED) shouldBe true
    driverProfile(None, advancedShardAwareness = Some(false))
      .getBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED) shouldBe false
  }
}
