# Documentation

## Connecting through ScyllaDB Cloud private endpoints

Some ScyllaDB Cloud clusters are only reachable through a private endpoint: AWS PrivateLink or GCP Private Service
Connect. The node addresses are not routable from your network; instead, the endpoint exposes every node as its own
`address:port` pair. ScyllaDB publishes that mapping in the `system.client_routes` table, keyed by a **connection
ID**, and the connector uses the driver's client routes support to reach every node through its route.

See the reference section for
[Client Routes (Private Endpoint) Connection Options](reference.md#client-routes-private-endpoint-connection-options).

### Prerequisites

* A ScyllaDB Cloud cluster with a private connection (AWS PrivateLink or GCP Private Service Connect), or
  ScyllaDB 2026.1 or newer with routes registered in `system.client_routes`. An older server refuses the connection
  with `Server does not support CLIENT_ROUTES_CHANGE event (requires ScyllaDB Enterprise >= 2026.1)`.
* The Spark driver and every executor run in a network that reaches the private endpoint.
* The address(es) and port of your private endpoint. The port is the one of the endpoint, not necessarily 9042 (the
  usual CQL port when connecting without client routes).
* A role that may read `system.client_routes`: the driver loads the routes with the role the connector logs in with
  (`spark.cassandra.auth.username`). A superuser can; any other role needs
  `GRANT SELECT ON system.client_routes TO <role>`, as ScyllaDB lets every role read `system.local` and
  `system.peers` but not this table. Without it, the driver logs `Failed to query client routes: ... no SELECT
  permission` and reaches every node at its own address.
* The connection IDs of the private connections the application may use. You provide them: the ScyllaDB Cloud console
  shows the client routes connection ID with the details of each private connection, and the connector does not look
  them up anywhere. A connection ID belongs to one availability zone of one datacenter: a datacenter can have one per
  availability zone, and a cluster can have several datacenters.

### Configuration

Client routes are enabled by `spark.cassandra.connection.clientRoutes.endpoints`, a comma-separated list of connection
IDs:

```bash
spark-submit \
  --conf spark.cassandra.connection.host=<private endpoint address>,<private endpoint address>,... \
  --conf spark.cassandra.connection.port=<private endpoint port> \
  --conf spark.cassandra.connection.clientRoutes.endpoints=<connection ID>,<connection ID>,... \
  --conf spark.cassandra.connection.clientRoutes.shardAwareness=true \
  --conf spark.cassandra.auth.username=<user> \
  --conf spark.cassandra.auth.password=<password> \
  ...
```

* The connection IDs must together route every node of the local datacenter, see
  [Datacenters and availability zones](#datacenters-and-availability-zones).
* `spark.cassandra.connection.clientRoutes.shardAwareness=true` enables advanced shard awareness, which ScyllaDB
  Cloud private endpoints support. It is off by default on the client side, see [Performance](#performance).
* `spark.cassandra.connection.host` is the address of your private endpoint and `spark.cassandra.connection.port` its
  port, which is not necessarily 9042. When you have several private endpoint addresses, for example one per
  availability zone, list them all, comma-separated: every listed address is a contact point, and the driver tries
  them in random order until one connects. An entry may carry its own port as `address:port`; the others use
  `spark.cassandra.connection.port`. The contact points serve the initial connection, and the driver's control
  connection when it reconnects after trying every known node; requests always reach a node through the address and
  port published for it.
* An entry `connectionId=address` replaces the address published for that connection ID by a hostname or IP address
  (without port). Use it when the published DNS name does not resolve in your network, for example with GCP Private
  Service Connect, or with AWS PrivateLink when private DNS is disabled on the VPC endpoint:
  `spark.cassandra.connection.clientRoutes.endpoints=<connection ID>=10.0.0.5`.
* When the nodes themselves serve CQL on another port than 9042, set
  `spark.cassandra.connection.clientRoutes.nativeTransportPort` to that port (not to the private endpoint port): the
  driver needs it to match the cluster's topology and status events to nodes.
* Client routes cannot be combined with `spark.cassandra.connection.config.cloud.path` or
  `spark.cassandra.connection.config.profile.path`.

The settings can also be given per cluster, for example for DataFrames:

```scala
import org.apache.spark.sql.cassandra._

spark.conf.set("private/spark.cassandra.connection.host", "<private endpoint address>,<private endpoint address>")
spark.conf.set("private/spark.cassandra.connection.port", "<private endpoint port>")
spark.conf.set("private/spark.cassandra.connection.clientRoutes.endpoints", "<connection ID>,<connection ID>")

spark.read.cassandraFormat("tab", "ks", "private").load()
```

or for a catalog, with `spark.sql.catalog.<catalog name>.spark.cassandra.connection.clientRoutes.endpoints`.

Connection IDs that contain `,` or `=` cannot be written in the SparkConf setting. Use the programmatic API instead:

```scala
import com.datastax.spark.connector.cql._

val conf = CassandraConnectorConf(sc.getConf).copy(clientRoutes = Some(ClientRoutesConf(Seq(
  ClientRouteEndpointConf("<connection ID of zone 1>"),
  ClientRouteEndpointConf("<connection ID of zone 2>", connectionAddr = Some("10.0.0.5"))))))
implicit val connector: CassandraConnector = new CassandraConnector(conf)
```

### Datacenters and availability zones

The connector only connects to the nodes of one datacenter, the local datacenter. A connection ID belongs to one
availability zone of one datacenter, so a datacenter can have several connection IDs, one per availability zone.

* List the connection ID of every availability zone of the local datacenter: together, the listed IDs must route
  every node of the local datacenter. The [route verification](#route-verification) checks it.
* Connection IDs of other datacenters may be listed too: their routes are not used.
* Without `spark.cassandra.connection.localDC`, the local datacenter is the one of the nodes that have a client
  route. When the listed connection IDs cover several datacenters, set `spark.cassandra.connection.localDC`; the
  connection otherwise fails with `Client routes cover datacenters X, Y; set spark.cassandra.connection.localDC`.
* When a node is published under several listed connection IDs, the driver keeps one route per node: the last usable
  route it reads from `system.client_routes`. The order of the IDs in the setting does not choose the endpoint.

For example, for a cluster with two datacenters of three availability zones each, used from `AWS_US_EAST_1`:

```
spark.cassandra.connection.host=<use1-az1 address>,<use1-az2 address>,<use1-az3 address>
spark.cassandra.connection.port=<private endpoint port>
spark.cassandra.connection.localDC=AWS_US_EAST_1
spark.cassandra.connection.clientRoutes.endpoints=<use1-az1>,<use1-az2>,<use1-az3>,<usw2-az1>,<usw2-az2>,<usw2-az3>
```

where `<use1-az1>` is the connection ID of the first availability zone of `AWS_US_EAST_1`, `<use1-az1 address>` the
private endpoint address you were given for it, and so on. Only the routes
of the three `AWS_US_EAST_1` connection IDs carry traffic. Without the `AWS_US_WEST_2` IDs, `localDC` could be left
out.

### TLS

With `spark.cassandra.connection.ssl.enabled=true` (see
[Cassandra SSL Connection Options](reference.md#cassandra-ssl-connection-options)), the connector reaches every node
through the `tls_port` of its route instead of `port`.

### Route verification

The driver does not fail when a connection ID matches no route, or when it cannot use a route: it skips a route
without an address or with an empty one (unless its connection ID has an address given as `connectionId=address`), and
a route without the port it needs (`tls_port` with TLS, `port` otherwise) or with a port outside 1-65535. A node
without a usable route falls back to its own address, which is not reachable through a private endpoint. To turn such
mistakes into an immediate error, the connector reads `system.client_routes` when the Spark driver connects, and fails
if a node of the local datacenter has no usable route:

```
Client routes: 1 of 3 node(s) in the local datacenter 'AWS_US_EAST_1' have no usable client route for connection ID(s)
<use1-az1>, <use1-az2> (host IDs: ...). Check that spark.cassandra.connection.clientRoutes.endpoints lists every
connection ID that serves this datacenter (a datacenter can have one per availability zone).
```

A node needs only one usable route: routes the driver skips do not affect the other routes. The check logs a warning
for every configured connection ID without any route. Set `spark.cassandra.connection.clientRoutes.verify=false` to
skip the check. Executors never run it.

### Performance

* Nodes behind a private endpoint are never local to an executor, so the size of the connection pool of each executor
  to each node of the local datacenter is `spark.cassandra.connection.remoteConnectionsPerExecutor` (by default 1),
  rounded up to a multiple of the node's shard count: the driver opens as many connections to every shard. A node
  with 8 shards gets 8 connections per executor for any value from 1 to 8, and 16 for 9 to 16, so for write-heavy
  jobs only a value above the shard count adds connections.
* Advanced shard awareness, where the driver opens each connection from a client source port chosen so that the
  connection lands on a given shard, is off by default on the client side, as in the driver. ScyllaDB Cloud private
  endpoints support it: set `spark.cassandra.connection.clientRoutes.shardAwareness=true`, as in the example above.
  Leave it `false` behind a proxy that does not preserve the client source port, such as one without Proxy Protocol
  v2; each new connection then lands on whichever shard the node picks. Basic shard awareness works either way: the
  driver sends each request to a connection of the shard that owns the data.
* Spark cannot schedule tasks on the nodes that hold the data: the preferred locations of the partitions are private
  endpoint addresses, which never match an executor host.
* `repartitionByCassandraReplica` still makes `partitionsPerHost` partitions per node of the local datacenter, also
  when the nodes share a private endpoint address.

### Troubleshooting

| Error | Cause |
|---|---|
| `Server does not support CLIENT_ROUTES_CHANGE event` | The server does not support client routes (ScyllaDB older than 2026.1). |
| `... have no usable client route for connection ID(s) ...` | A wrong or missing connection ID (every availability zone of the local datacenter needs its own), or routes without an address (add `=<address>` to their connection ID), or without a valid `tls_port` (TLS) or `port`, whichever the connection needs. |
| `... no node of the local datacenter 'X' could be reached to read system.client_routes ...` | No node of the local datacenter is reachable, usually because none of them has a usable route (same causes as above), or `spark.cassandra.connection.localDC` names another datacenter than the connection IDs serve, or the role may not read `system.client_routes` (see below). |
| `... no SELECT permission on <table system.client_routes> ...`, or `Failed to query client routes` in the driver log | The role the connector logs in with may not read the routes: `GRANT SELECT ON system.client_routes TO <role>`. |
| `Client routes: no nodes found in the local datacenter 'X'` | `spark.cassandra.connection.localDC` names no datacenter of the cluster: the error lists the known ones. |
| `Client routes cover datacenters X, Y` | The connection IDs cover several datacenters: set `spark.cassandra.connection.localDC`. |
| `Cannot determine the local datacenter: no node has a usable client route ...` | No route is usable and no contact point matches a node: check the connection IDs and ports, or set `spark.cassandra.connection.localDC` to get the report of the nodes without a usable route. |
| `DNS resolution failed for host_id=...` | The address published for a route does not resolve: add `=<address>` to its connection ID. |
