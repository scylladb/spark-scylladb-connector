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

import com.datastax.dse.driver.api.core.DseProtocolVersion
import com.datastax.oss.driver.api.core.{DefaultProtocolVersion, ProtocolVersion}
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector.util.schemaFromCassandra

class BoundStatementBuilderSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {
  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass: Unit = {
    conn.withSessionDo { session =>
      createKeyspace(session, ks)
      session.execute( s"""CREATE TABLE IF NOT EXISTS "$ks".tab (id INT PRIMARY KEY, value TEXT)""")
      session.execute(
        s"""CREATE TABLE IF NOT EXISTS "$ks".autots_marker_collision
           |(id INT PRIMARY KEY, ${TableWriter.AutoTimestampParam} TEXT)""".stripMargin)
    }
  }

  lazy val schema = schemaFromCassandra(conn, Some(ks), Some("tab"))
  lazy val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, CassandraOption[String])]
    .rowWriter(schema.tables.head, IndexedSeq("id", "value"))
  lazy val valueRowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String)]
    .rowWriter(schema.tables.head, IndexedSeq("id", "value"))
  lazy val ps = conn.withSessionDo(session =>
    session.prepare( s"""INSERT INTO "$ks".tab (id, value) VALUES (?, ?) """))
  lazy val autoTimestampPs = conn.withSessionDo(session =>
    session.prepare(
      s"""UPDATE "$ks".tab USING TIMESTAMP :${TableWriter.AutoTimestampParam}
         |SET value = :value WHERE id = :id""".stripMargin))
  lazy val markerCollisionSchema = schemaFromCassandra(conn, Some(ks), Some("autots_marker_collision"))
  lazy val markerCollisionRowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String)]
    .rowWriter(markerCollisionSchema.tables.head, IndexedSeq("id", TableWriter.AutoTimestampParam))
  lazy val markerCollisionPs = conn.withSessionDo(session =>
    session.prepare(
      s"""INSERT INTO "$ks".autots_marker_collision
         |(id, ${TableWriter.AutoTimestampParam}) VALUES (?, ?)""".stripMargin))

  val PVGt4 = Seq(DefaultProtocolVersion.V4, DefaultProtocolVersion.V5, DseProtocolVersion.DSE_V1, DseProtocolVersion.DSE_V2)
  val PVLte3 = Seq(DefaultProtocolVersion.V3)
  //TODO Switch to ```((InternalDriverContext)session.getContext()).getProtocolVersionRegistry()```

  "BoundStatementBuilder" should "ignore Unset values if ProtocolVersion >= 4" in {
    for (testProtocol <- PVGt4) {
      val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = testProtocol)
      val x = bsb.bind((1, CassandraOption.Unset))
      withClue(s"$testProtocol should ignore unset values :")(x.stmt.isSet("value") should be(false))
    }
  }

  it should "set Unset values to null if ProtocolVersion <= 3" in {
    for (testProtocol <- PVLte3) {
      val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = testProtocol)
      val x = bsb.bind((1, CassandraOption.Unset))
      withClue(s"$testProtocol should set to null :")(x.stmt.isNull("value") should be(true))
    }
  }

  it should "ignore null values if ignoreNulls is set and protocol version >= 4" in {
    val testProtocols = PVGt4
    for (testProtocol <- testProtocols) {
      val bsb = new BoundStatementBuilder(
        rowWriter,
        ps,
        protocolVersion = testProtocol,
        ignoreNulls = true)

      val x = bsb.bind((1, null))
      withClue(s"$testProtocol should ignore unset values :")(x.stmt.isSet("value") should be(false))
    }
  }

  it should "throw an exception if ignoreNulls is set and protocol version <= 3" in {
    for (testProtocol <- PVLte3) {
      intercept[IllegalArgumentException] {
        val bsb = new BoundStatementBuilder(
          rowWriter,
          ps,
          protocolVersion = testProtocol,
          ignoreNulls = true)
      }
    }
  }

  it should "not bind connector_autots unless auto timestamp binding is requested" in {
    val bsb = new BoundStatementBuilder(
      markerCollisionRowWriter,
      markerCollisionPs,
      protocolVersion = conn.withSessionDo(_.getContext.getProtocolVersion))

    val bound = bsb.bind((1, "user-value")).stmt
    bound.getString(TableWriter.AutoTimestampParam) should be("user-value")
  }

  it should "bind connector_autots when auto timestamp binding is requested" in {
    val bsb = new BoundStatementBuilder(
      valueRowWriter,
      autoTimestampPs,
      protocolVersion = conn.withSessionDo(_.getContext.getProtocolVersion),
      autoTimestampParam = Some(TableWriter.AutoTimestampParam))

    val bound = bsb.bind((1, "value")).stmt
    bound.getLong(TableWriter.AutoTimestampParam) should be > 0L
  }
}
