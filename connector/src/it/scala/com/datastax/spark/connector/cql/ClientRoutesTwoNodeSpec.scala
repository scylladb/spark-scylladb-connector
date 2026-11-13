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

import com.datastax.oss.driver.internal.core.metadata.ClientRoutesEndPoint
import com.datastax.spark.connector._
import com.datastax.spark.connector.cluster.TwoNodeCluster
import com.datastax.spark.connector.cql.CassandraConnectorConf.ClientRoutesEndpointsParam

/** Client routes against two ScyllaDB nodes, each published under its own connection ID, like the nodes of two
  * availability zones that have one private endpoint each.
  *
  * Every route maps a node to its shard-aware port, so sessions that honour the routes reach the nodes through a port
  * that differs from the contact point's.
  */
class ClientRoutesTwoNodeSpec extends SparkCassandraITFlatSpecBase with TwoNodeCluster with ClientRoutesTestSupport {

  override lazy val conn = CassandraConnector(defaultConf)

  private val zoneConnectionIds = Seq("spark-connector-it-az1", "spark-connector-it-az2")
  private val allZones = zoneConnectionIds.mkString(",")
  private lazy val routes = nodes.zip(zoneConnectionIds).map { case ((hostId, address), connectionId) =>
    routeEntry(connectionId, port = Some(shardAwarePort), tlsPort = None, address, hostId)
  }
  private lazy val routedAddresses =
    nodes.map { case (_, address) => new InetSocketAddress(address, shardAwarePort) }.toSet

  private val rows = (1 to 100).map(i => (i, s"value $i"))

  override def beforeClass: Unit = {
    if (clientRoutesSupported) {
      conn.withSessionDo { session =>
        createKeyspace(session)
        session.execute(s"CREATE TABLE $ks.kv (key INT PRIMARY KEY, value TEXT)")
      }
      registerRoutes(routes: _*)
    }
  }

  override def afterClass: Unit = {
    if (clientRoutesSupported) deregisterRoutes(routes: _*)
  }

  "A connector configured with the connection ID of every availability zone" should
    "reach every node through its own route" in {
    assumeClientRoutes()
    nodes should have size 2

    val endPoints = nodeEndPoints(CassandraConnector(routedConf(allZones)))

    all(endPoints) shouldBe a[ClientRoutesEndPoint]
    endPoints.map(_.resolve()).toSet shouldBe routedAddresses
  }

  it should "use the datacenter of the routed nodes when no local datacenter is set" in {
    assumeClientRoutes()
    val conf = defaultConf.set(ClientRoutesEndpointsParam.name, allZones)

    nodeEndPoints(CassandraConnector(conf)).map(_.resolve()).toSet shouldBe routedAddresses
  }

  it should "name the node of the availability zone whose connection ID is missing" in {
    assumeClientRoutes()
    val (unroutedHostId, _) = nodes(1)

    val error = intercept[IOException](
      CassandraConnector(routedConf(zoneConnectionIds.head)).withSessionDo(identity))

    error.getMessage should include("1 of 2 node(s)")
    error.getMessage should include(s"(host IDs: $unroutedHostId)")
    error.getMessage should include("lists every connection ID that serves this datacenter")
  }

  it should "scan a table, repartition by replica and join through the routes" in {
    assumeClientRoutes()
    implicit val connector: CassandraConnector = CassandraConnector(routedConf(allZones))
    sc.parallelize(rows).saveToCassandra(ks, "kv", SomeColumns("key", "value"))

    sc.cassandraTable[(Int, String)](ks, "kv").collect().toSet shouldBe rows.toSet

    val keys = sc.parallelize(rows.map { case (key, _) => Tuple1(key) })
    val repartitioned = keys.repartitionByCassandraReplica(ks, "kv", partitionsPerHost = 3)
    // partitionsPerHost partitions for each of the two nodes
    repartitioned.partitions.length shouldBe 6
    repartitioned.partitions.flatMap(repartitioned.preferredLocations).toSet should
      contain allElementsOf nodes.map { case (_, address) => address }
    repartitioned.joinWithCassandraTable[(Int, String)](ks, "kv").values.collect().toSet shouldBe rows.toSet
  }
}
