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
import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED
import com.datastax.oss.driver.api.core.metadata.EndPoint
import com.datastax.oss.driver.internal.core.context.InternalDriverContext
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesEndPoint
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnectorConf.{ClientRoutesEndpointsParam, ClientRoutesVerifyParam, ConnectionHostParam, LocalDCParam}
import com.datastax.spark.connector.datasource.CassandraSourceUtil
import org.apache.spark.sql.cassandra._

import scala.jdk.CollectionConverters._

/** Client routes (ScyllaDB Cloud private endpoints) against a single ScyllaDB node without TLS.
  *
  * The route maps the node to its shard-aware port, so sessions that honour it reach the node through a port that
  * differs from the contact point's.
  */
class ClientRoutesSpec extends SparkCassandraITFlatSpecBase with DefaultCluster with ClientRoutesTestSupport {

  override lazy val conn = CassandraConnector(defaultConf)

  private val connectionId = "spark-connector-it"
  // A second private endpoint, e.g. of another availability zone, that also publishes the node
  private val otherZoneConnectionId = "spark-connector-it-az2"
  // A route that ScyllaDB accepts but the driver skips without an address override
  private val emptyAddressConnectionId = "spark-connector-it-empty-address"
  private lazy val routes = Seq(connectionId, otherZoneConnectionId)
    .map(routeEntry(_, port = Some(shardAwarePort), tlsPort = None)) :+
    routeEntry(emptyAddressConnectionId, port = Some(shardAwarePort), tlsPort = None, address = "")

  override def beforeClass: Unit = {
    if (clientRoutesSupported) {
      conn.withSessionDo { session =>
        createKeyspace(session)
        session.execute(s"CREATE TABLE $ks.kv_rdd (key INT PRIMARY KEY, value TEXT)")
        session.execute(s"CREATE TABLE $ks.kv_df (key INT PRIMARY KEY, value TEXT)")
      }
      registerRoutes(routes: _*)
    }
  }

  override def afterClass: Unit = {
    if (clientRoutesSupported) deregisterRoutes(routes: _*)
  }

  private def assertRouted(endPoints: Seq[EndPoint]): Unit = {
    endPoints should not be empty
    all(endPoints) shouldBe a[ClientRoutesEndPoint]
    endPoints.map(_.resolve()) shouldBe Seq(new InetSocketAddress(nodeAddress, shardAwarePort))
  }

  private val rows = (1 to 100).map(i => (i, s"value $i"))

  "A connector configured with client routes" should "reach the nodes through the routed endpoint" in {
    assumeClientRoutes()
    assertRouted(nodeEndPoints(CassandraConnector(routedConf(connectionId))))
  }

  it should "use the datacenter of the routed nodes when no local datacenter is set" in {
    assumeClientRoutes()
    // The contact point's address:port matches no node's routed address, so it cannot tell the local datacenter
    val conf = defaultConf.set(ClientRoutesEndpointsParam.name, connectionId)

    assertRouted(nodeEndPoints(CassandraConnector(conf)))
  }

  it should "not share a session with a connector without client routes" in {
    assumeClientRoutes()
    val routedConnectorConf = routedConf(connectionId)
    // Differs from the routed connector by the client routes only
    val plain = CassandraConnector(routedConnectorConf.clone().remove(ClientRoutesEndpointsParam.name))
    def endPoints(session: CqlSession): Seq[EndPoint] =
      session.getMetadata.getNodes.values.asScala.toSeq.map(_.getEndPoint)

    // While the routed session is open, a cache that ignored the client routes would return it for the plain connector
    CassandraConnector(routedConnectorConf).withSessionDo { routedSession =>
      plain.withSessionDo { plainSession =>
        plainSession.getContext should not be theSameInstanceAs(routedSession.getContext)
        all(endPoints(plainSession)) should not be a[ClientRoutesEndPoint]
      }
      all(endPoints(routedSession)) shouldBe a[ClientRoutesEndPoint]
    }
  }

  it should "write and read an RDD" in {
    assumeClientRoutes()
    implicit val connector: CassandraConnector = CassandraConnector(routedConf(connectionId))

    sc.parallelize(rows).saveToCassandra(ks, "kv_rdd", SomeColumns("key", "value"))

    sc.cassandraTable[(Int, String)](ks, "kv_rdd").collect().toSet shouldBe rows.toSet
    assertRouted(nodeEndPoints(connector))
  }

  it should "repartition by replica and join through the routes" in {
    assumeClientRoutes()
    implicit val connector: CassandraConnector = CassandraConnector(routedConf(connectionId))
    sc.parallelize(rows).saveToCassandra(ks, "kv_rdd", SomeColumns("key", "value"))
    val keys = sc.parallelize(rows.map { case (key, _) => Tuple1(key) })

    val repartitioned = keys.repartitionByCassandraReplica(ks, "kv_rdd", partitionsPerHost = 3)

    repartitioned.partitions.length shouldBe 3
    all(repartitioned.partitions.map(repartitioned.preferredLocations)) should contain(nodeAddress)
    repartitioned.joinWithCassandraTable[(Int, String)](ks, "kv_rdd").values.collect().toSet shouldBe rows.toSet
    assertRouted(nodeEndPoints(connector))
  }

  it should "write and read a DataFrame configured with cluster level settings" in {
    assumeClientRoutes()
    val clusterName = "routed"
    withConfig(
      s"$clusterName/${ClientRoutesEndpointsParam.name}" -> connectionId,
      s"$clusterName/${LocalDCParam.name}" -> datacenter) {
      import spark.implicits._

      rows.toDF("key", "value").write.cassandraFormat("kv_df", ks, clusterName).mode("append").save()

      spark.read.cassandraFormat("kv_df", ks, clusterName).load().as[(Int, String)].collect().toSet shouldBe rows.toSet
      val clusterConf = CassandraSourceUtil.consolidateConfs(sc.getConf, spark.conf.getAll, clusterName)
      assertRouted(nodeEndPoints(CassandraConnector(clusterConf)))
    }
  }

  it should "fail on the Spark driver when no route matches the connection ID" in {
    assumeClientRoutes()
    val unknownId = "spark-connector-it-unknown"

    val error = intercept[IOException](CassandraConnector(routedConf(unknownId)).withSessionDo(identity))

    error.getMessage should include("have no usable client route")
    error.getMessage should include(unknownId)
    error.getCause shouldBe an[IllegalStateException]
  }

  it should "report missing routes even when no local datacenter is set" in {
    assumeClientRoutes()
    val unknownId = "spark-connector-it-unknown"
    // Without routes, the control node keeps the contact point as its address, which identifies the local datacenter
    val conf = defaultConf.set(ClientRoutesEndpointsParam.name, unknownId)

    val error = intercept[IOException](CassandraConnector(conf).withSessionDo(identity))

    error.getMessage should include("have no usable client route")
    error.getMessage should include(unknownId)
  }

  it should "report a local datacenter without nodes rather than fail to query it" in {
    assumeClientRoutes()
    val conf = routedConf(connectionId).set(LocalDCParam.name, "no-such-dc")

    val error = intercept[IOException](CassandraConnector(conf).withSessionDo(identity))

    error.getMessage should include("no nodes found in the local datacenter 'no-such-dc'")
  }

  it should "skip a route with an empty address and keep the other connection ID's route of the node" in {
    assumeClientRoutes()

    assertRouted(nodeEndPoints(CassandraConnector(routedConf(s"$emptyAddressConnectionId,$connectionId"))))
  }

  it should "fail on the Spark driver when the node's only route has an empty address" in {
    assumeClientRoutes()

    val error = intercept[IOException](CassandraConnector(routedConf(emptyAddressConnectionId)).withSessionDo(identity))

    error.getMessage should include("have no usable client route")
    error.getMessage should include("have no address, or an empty one")
  }

  it should "use the connection's address override in place of an empty published address" in {
    assumeClientRoutes()

    assertRouted(nodeEndPoints(CassandraConnector(routedConf(s"$emptyAddressConnectionId=$nodeAddress"))))
  }

  it should "open the session without a usable route when the verification is disabled" in {
    assumeClientRoutes()
    val conf = routedConf("spark-connector-it-unknown").set(ClientRoutesVerifyParam.name, "false")

    // Without a route the driver falls back to the node's own address, which the verification normally rejects
    nodeEndPoints(CassandraConnector(conf)).map(_.resolve()) should not contain
      new InetSocketAddress(nodeAddress, shardAwarePort)
  }

  it should "accept several connection IDs, including IDs that publish the same node and IDs without routes" in {
    assumeClientRoutes()
    val connectionIds = Seq(connectionId, otherZoneConnectionId, "spark-connector-it-other-dc").mkString(",")

    assertRouted(nodeEndPoints(CassandraConnector(routedConf(connectionIds))))
  }

  it should "turn the driver's advanced shard awareness off unless shardAwareness is set" in {
    assumeClientRoutes()
    def advancedShardAwareness(connector: CassandraConnector): Boolean = connector.withSessionDo(
      _.getContext.getConfig.getDefaultProfile.getBoolean(CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED))

    advancedShardAwareness(CassandraConnector(routedConf(connectionId))) shouldBe false
    advancedShardAwareness(conn) shouldBe true
  }

  it should "use every listed private endpoint address as a contact point" in {
    assumeClientRoutes()
    val unreachable = new InetSocketAddress("127.0.0.1", 1) // e.g. the endpoint of an availability zone that is down
    val routed = new InetSocketAddress(nodeAddress, shardAwarePort)
    val conf = routedConf(connectionId).set(ConnectionHostParam.name, s"127.0.0.1:1,$nodeAddress:$shardAwarePort")
    val connector = CassandraConnector(conf)

    // The driver tries the contact points in random order until one connects
    val contactPoints = connector.withSessionDo { session =>
      session.getContext.asInstanceOf[InternalDriverContext].getMetadataManager.getContactPoints.asScala
        .map(_.getEndPoint.resolve()).toSet
    }
    contactPoints shouldBe Set(unreachable, routed)
    assertRouted(nodeEndPoints(connector))
  }
}
