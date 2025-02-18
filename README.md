# DataStax Connector for Apache Spark to Apache Cassandra

*Lightning-fast cluster computing with Apache Spark&trade; and Apache Cassandra&reg;.*

[![CI](https://github.com/datastax/spark-cassandra-connector/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/datastax/spark-cassandra-connector/actions?query=branch%3Amaster)

This is a fork from [datastax/spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector) including features specific to [ScyllaDB](https://scylladb.com) and to the needs of the [ScyllaDB Migrator](https://migrator.docs.scylladb.com).

## Changes compared to the original library

- Add support for skipping some token ranges when reading a table, and track into a Spark accumulator the token ranges that have been written.

The complete changelog can be viewed here: [master...scylla-4.x](https://github.com/datastax/spark-cassandra-connector/compare/master...scylladb:spark-scylladb-connector:scylla-4.x).

## Quick Links

| What       | Where                                                                                                                                                                                                                                                                                                               |
| ---------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Community  | Chat with us at [Apache Cassandra](https://cassandra.apache.org/_/community.html#discussions)                                                                                                                                                                                                                       |
| Scala Docs | Most Recent Release (3.5.1): [Connector API docs](https://datastax.github.io/spark-cassandra-connector/ApiDocs/3.5.1/connector/com/datastax/spark/connector/index.html), [Connector Driver docs](https://datastax.github.io/spark-cassandra-connector/ApiDocs/3.5.1/driver/com/datastax/spark/connector/index.html) |
| Latest Production Release | [3.5.1](https://search.maven.org/artifact/com.datastax.spark/spark-cassandra-connector_2.12/3.5.1/jar)                                                                                                                                                                                                              |

## News
### 3.5.1
 - The latest release of the Spark-Cassandra-Connector introduces support for vector types, greatly enhancing its capabilities. This new feature allows developers to seamlessly integrate and work with Cassandra 5.0 and Astra vectors within the Spark ecosystem. By supporting vector types, the connector now provides insights into AI and Retrieval-Augmented Generation (RAG) data, enabling more advanced and efficient data processing and analysis.
 
## Features

This library lets you expose Cassandra tables as Spark RDDs and Datasets/DataFrames, write
Spark RDDs and Datasets/DataFrames to Cassandra tables, and execute arbitrary CQL queries
in your Spark applications.

 - Compatible with Apache Cassandra version 2.1 or higher (see table below)
 - Compatible with Apache Spark 1.0 through 3.5 ([see table below](#version-compatibility))
 - Compatible with Scala 2.11, 2.12 and 2.13
 - Exposes Cassandra tables as Spark RDDs and Datasets/DataFrames
 - Maps table rows to CassandraRow objects or tuples
 - Offers customizable object mapper for mapping rows to objects of user-defined classes
 - Saves RDDs back to Cassandra by implicit `saveToCassandra` call
 - Delete rows and columns from cassandra by implicit `deleteFromCassandra` call
 - Join with a subset of Cassandra data using `joinWithCassandraTable` call for RDDs, and
   optimizes join with data in Cassandra when using Datasets/DataFrames
 - Partition RDDs according to Cassandra replication using `repartitionByCassandraReplica` call
 - Converts data types between Cassandra and Scala
 - Supports all Cassandra data types including collections
 - Filters rows on the server side via the CQL `WHERE` clause
 - Allows for execution of arbitrary CQL statements
 - Plays nice with Cassandra Virtual Nodes
 - Could be used in all languages supporting Datasets/DataFrames API: Python, R, etc.

## Version Compatibility

The connector project has several branches, each of which map into different
supported versions of  Spark and Cassandra. For previous releases the branch is
named "bX.Y" where X.Y is the major+minor version; for example the "b1.6" branch
corresponds to the 1.6 release. The "master" branch will normally contain
development for the next connector release in progress.

Currently, the following branch is actively supported: 
4.x ([scylla-4.x](https://github.com/scylladb/spark-scylladb-connector/tree/scylla-4.x)).

| Connector | Spark         | Cassandra                  | Cassandra Java Driver | Minimum Java Version | Supported Scala Versions |
|-----------|---------------|----------------------------|-----------------------|----------------------|--------------------------|
| 4.0.0     | 3.5.x         | 2.1.5*, 2.2, 3.x, 4.x, 5.0 | 4.18.1                | 8                    | 2.12, 2.13               |

## Hosted API Docs
API documentation for the Scala and Java interfaces are available online:

- [Latest](https://javadoc.io/doc/com.scylladb/spark-scylladb-connector_2.13/latest/index.html).

## Download

This project is available on the Maven Central Repository.
For SBT to download the connector binaries, sources and javadoc, put this in your project
SBT config:

    libraryDependencies += "com.scylladb" %% "spark-scylladb-connector" % "4.0.0"

* The default Scala version for Spark 3.0+ is 2.12 please choose the appropriate build. See the
[FAQ](doc/FAQ.md) for more information.

## Building
See [Building And Artifacts](doc/12_building_and_artifacts.md)

## Documentation

  - [Quick-start guide](doc/0_quick_start.md)
  - [Connecting to Cassandra](doc/1_connecting.md)
  - [Loading datasets from Cassandra](doc/2_loading.md)
  - [Server-side data selection and filtering](doc/3_selection.md)   
  - [Working with user-defined case classes and tuples](doc/4_mapper.md)
  - [Saving and deleting datasets to/from Cassandra](doc/5_saving.md)
  - [Customizing the object mapping](doc/6_advanced_mapper.md)
  - [Using Connector in Java](doc/7_java_api.md)
  - [Spark Streaming with Cassandra](doc/8_streaming.md)
  - [The spark-cassandra-connector-embedded Artifact](doc/10_embedded.md)
  - [Performance monitoring](doc/11_metrics.md)
  - [Building And Artifacts](doc/12_building_and_artifacts.md)
  - [The Spark Shell](doc/13_spark_shell.md)
  - [DataFrames](doc/14_data_frames.md)
  - [Python](doc/15_python.md)
  - [Partitioner](doc/16_partitioning.md)
  - [Submitting applications](doc/17_submitting.md)
  - [Frequently Asked Questions](doc/FAQ.md)
  - [Configuration Parameter Reference Table](doc/reference.md)
  - [Tips for Developing the Spark Cassandra Connector](doc/developers.md)

## Online Training

In [DS320: Analytics with Spark](https://www.youtube.com/watch?v=D6PMEQAfjeU&list=PL2g2h-wyI4Sp0bqsw_IRYu1M4aPkt045x), you will learn how to effectively and efficiently solve analytical problems with Apache Spark, Apache Cassandra, and DataStax Enterprise. You will learn about Spark API, Spark-Cassandra Connector, Spark SQL, Spark Streaming, and crucial performance optimization techniques.

## Community

### Reporting Bugs

New issues may be reported using [JIRA](https://datastax-oss.atlassian.net/browse/SPARKC/). Please include
all relevant details including versions of Spark, Spark Cassandra Connector, Cassandra and/or DSE. A minimal
reproducible case with sample code is ideal.

### Mailing List

Questions and requests for help may be submitted to the [user mailing list](https://groups.google.com/a/lists.datastax.com/forum/#!forum/spark-connector-user).


## Q/A Exchange

The [DataStax Community](https://community.datastax.com/index.html) provides a free question and answer website for any and all
questions relating to any DataStax Related technology. Including the Spark Cassandra Connector. Both DataStax 
engineers and community members frequent this board and answer questions.

## Contributing

To protect the community, all contributors are required to sign the [DataStax Spark Cassandra Connector Contribution License Agreement](https://cla.datastax.com/). The process is completely electronic and should only take a few minutes.

To develop this project, we recommend using IntelliJ IDEA. Make sure you have
installed and enabled the Scala Plugin. Open the project with IntelliJ IDEA and
it will automatically create the project structure from the provided SBT
configuration.

[Tips for Developing the Spark Cassandra Connector](doc/developers.md)

Checklist for contributing changes to the project:
* Create a [SPARKC JIRA](https://datastax-oss.atlassian.net/projects/SPARKC/issues)
* Make sure that all unit tests and integration tests pass
* Add an appropriate entry at the top of CHANGES.txt
* If the change has any end-user impacts, also include changes to the ./doc files as needed
* Prefix the pull request description with the JIRA number, for example: "SPARKC-123: Fix the ..."
* Open a pull-request on GitHub and await review

## Testing
To run unit and integration tests:

    ./sbt/sbt test
    ./sbt/sbt it:test

Note that the integration tests require [CCM](https://github.com/riptano/ccm) to be installed on your machine.
See [Tips for Developing the Spark Cassandra Connector](doc/developers.md) for details.

By default, integration tests start up a separate, single Cassandra instance and run Spark in local mode.
It is possible to run integration tests with your own Spark cluster.
First, prepare a jar with testing code:

    ./sbt/sbt test:package

Then copy the generated test jar to your Spark nodes and run:    

    export IT_TEST_SPARK_MASTER=<Spark Master URL>
    ./sbt/sbt it:test

## Generating Documents
To generate the Reference Document use

    ./sbt/sbt spark-cassandra-connector-unshaded/run (outputLocation)

outputLocation defaults to doc/reference.md

## Branching Model

Our branch `scylla-4.x` is based off commit `dbbf02890605692d163572cda4b2462993754d7b`. It introduces binary incompatible changes compared to the upstream version 3.5.x.

We should occasionally merge the upstream changes to our fork.

## Release Process

Create a [new GitHub release](https://github.com/scylladb/spark-scylladb-connector/releases), give it a tag name (please see the rules below), a title, and a description. You can generate the changelog automatically from the GitHub UI. Click Publish. A workflow will be automatically triggered and will build the project and release it on [Sonatype](https://central.sonatype.org).

Rules for the release tag name:

- Make sure to use tag names like `v1.2.3`, starting with `v` and followed by a [semantic version number](https://semver.org).
- Bump the major version number if the new release breaks the backward compatibility (e.g., an existing configuration or setup will not work anymore with the new release).
- Bump the minor version number if the new release introduces new features in a backward compatible manner.
- Bump the patch version number if the new release only introduces bugfixes in a backward compatible manner.

## License

Copyright DataStax, Inc. Copyright ScyllaDB.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
