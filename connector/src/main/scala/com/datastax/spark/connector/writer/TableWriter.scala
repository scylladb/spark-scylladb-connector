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

import java.io.{Closeable, IOException}
import java.util.function.Supplier

import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.core.cql.{DefaultBatchType, PreparedStatement, SimpleStatement}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CqlTokenRange}
import com.datastax.spark.connector.types.{ListType, MapType}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util._
import com.datastax.spark.connector.writer.AsyncExecutor.Handler
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.metrics.OutputMetricsUpdater

import scala.collection._
import scala.util.control.NonFatal

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class TableWriter[T] private (
    connector: CassandraConnector,
    tableDef: TableDef,
    columnSelector: IndexedSeq[ColumnRef],
    rowWriter: RowWriter[T],
    writeConf: WriteConf,
    partitions: Array[Partition],
    tokenRangeAcc: Option[TokenRangeAccumulator],
    private val isDelete: Boolean = false,
    private var cachedDeleteQueryTemplate: Option[String] = None,
    syntheticColumnNames: Set[String] = Set.empty,
    autoAddedOptionColumnNames: Set[String] = Set.empty) extends Serializable with Logging {

  require(!tableDef.isView,
    s"${tableDef.name} is a Materialized View and Views are not writable")

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  private val deleteOptionColumnNames =
    if (isDelete) TableWriter.nonPrimaryKeyOptionPlaceholders(tableDef, writeConf) else Set.empty[String]
  private val bindOnlyColumnNames = syntheticColumnNames ++ autoAddedOptionColumnNames ++ deleteOptionColumnNames
  val columnNames = rowWriter.columnNames.filterNot(bindOnlyColumnNames.contains)
  val columns = columnNames.map(tableDef.columnByName)

  private lazy val tableDefWithoutPlaceholders: TableDef =
    if (syntheticColumnNames.isEmpty) tableDef
    else tableDef.copy(
      regularColumns = tableDef.regularColumns.filterNot(c => syntheticColumnNames.contains(c.columnName))
    )

  private[connector] lazy val queryTemplateUsingInsert: String = {
    val rowWriterColumnNames = rowWriter.columnNames.toSet
    val unboundOptionPlaceholders = writeConf.optionPlaceholders.filterNot(rowWriterColumnNames.contains)
    if (unboundOptionPlaceholders.nonEmpty) {
      throw new IllegalArgumentException(
        s"INSERT uses per-row write option placeholder(s) that are not selected from the input row: " +
          s"${unboundOptionPlaceholders.mkString(", ")}. Add the placeholder column(s) to the column selector or use AllColumns.")
    }

    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ifNotExistsSpec = if (writeConf.ifNotExists) "IF NOT EXISTS " else ""

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $ifNotExistsSpec$optionsSpec".trim
  }

  // A `def` because `deleteColumns` is a parameter, not a property of the writer.
  // Called once by `TableWriter.forDelete` which caches the result in `cachedDeleteQueryTemplate`.
  // Precondition: `columns` (the key columns for the WHERE clause) must contain only primary key
  // columns. This is enforced by the caller (RDDFunctions.deleteFromCassandra passes PK-only
  // columns), and validated by the primary-key-only check below.
  private[writer] def deleteQueryTemplate(deleteColumns: ColumnSelector): String = {
    // CQL DELETE supports deleting an entire collection column, but not collection-element
    // write operations (append, prepend, remove).
    deleteColumns match {
      case sc: SomeColumns =>
        val collectionRefs = sc.columns.collect {
          case c: CollectionColumnName if c.collectionBehavior != CollectionOverwrite => c.columnName
        }
        if (collectionRefs.nonEmpty) {
          throw new IllegalArgumentException(
            s"CQL DELETE does not support collection behaviors. Collection columns found in delete columns: ${collectionRefs.mkString(", ")}")
        }
      case _ => // AllColumns, PrimaryKeyColumns, etc. never produce CollectionColumnName
    }

    // Validate column existence before selectFrom to give a DELETE-specific error message.
    // selectFrom throws a generic NoSuchElementException that doesn't mention DELETE.
    deleteColumns match {
      case sc: SomeColumns =>
        val tableColumnNames = tableDefWithoutPlaceholders.columns.map(_.columnName).toSet
        val missing = sc.columns.map(_.columnName).filterNot(tableColumnNames.contains)
        if (missing.nonEmpty)
          throw new IllegalArgumentException(
            s"Column(s) not found for DELETE in table ${tableDef.tableName}: ${missing.mkString(", ")}")
      case _ =>
    }

    val primaryKeyNames = tableDef.primaryKey.map(_.columnName).toSet
    // selectFrom uses tableDefWithoutPlaceholders so that synthetic option-placeholder columns are excluded
    val allDeleteColumnNames: Seq[String] = deleteColumns.selectFrom(tableDefWithoutPlaceholders).map(_.columnName)
    // When AllColumns is used, auto-filter primary key columns since they can't be deleted
    val deleteColumnNames = if (deleteColumns == AllColumns) {
      val filtered = allDeleteColumnNames.filterNot(primaryKeyNames.contains)
      if (filtered.isEmpty)
        logInfo(s"Table ${tableDef.tableName} has no non-PK columns; AllColumns delete will perform a full-row delete")
      filtered
    } else {
      allDeleteColumnNames
    }
    val deleteKeyColumns = columns.filterNot { col =>
      TableWriter.nonPrimaryKeyOptionPlaceholders(tableDef, writeConf).contains(col.columnName)
    }
    val (primaryKey, regularColumns) = deleteKeyColumns.partition(_.isPrimaryKeyColumn)
    if (regularColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"Only primary key columns can be specified as key columns for DELETE. Regular columns found: ${regularColumns.mkString(", ")}")
    }
    // Empty deleteColumnNames = full row delete; PK column check only applies to column-level deletes
    if (deleteColumnNames.nonEmpty) {
      val pkInDelete = deleteColumnNames.filter(primaryKeyNames.contains)
      if (pkInDelete.nonEmpty) {
        throw new IllegalArgumentException(
          s"Primary key columns cannot be specified as delete columns. Primary key columns found: ${pkInDelete.mkString(", ")}")
      }
    }
    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val deleteColumnsClause = deleteColumnNames.map(quote).mkString(", ")
    val deleteColumnDefs = deleteColumnNames.map(tableDef.columnByName)
    if (deleteColumnDefs.exists(col => !col.isStatic)) {
      val selectedPrimaryKeyNames = primaryKey.map(_.columnName).toSet
      val missingPrimaryKeyNames = tableDef.primaryKey.map(_.columnName).filterNot(selectedPrimaryKeyNames.contains)
      if (missingPrimaryKeyNames.nonEmpty) {
        throw new IllegalArgumentException(
          s"Deleting non-static columns requires the full primary key. " +
            s"Missing primary key columns: ${missingPrimaryKeyNames.mkString(", ")}")
      }
    }
    val whereColumns =
      if (deleteColumnDefs.nonEmpty && deleteColumnDefs.forall(_.isStatic))
        primaryKey.filter(_.isPartitionKeyColumn)
      else
        primaryKey
    val whereClause = quotedColumnNames(whereColumns).map(c => s"$c = :$c").mkString(" AND ")

    val usingTimestampClause = timestampSpec.map(ts => s" USING $ts").getOrElse("")
    val deleteSpec = if (deleteColumnsClause.nonEmpty) s"$deleteColumnsClause " else ""

    s"DELETE ${deleteSpec}FROM ${quote(keyspaceName)}.${quote(tableName)}$usingTimestampClause WHERE $whereClause"
  }

  private[writer] lazy val queryTemplateUsingUpdate: String = {
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    val nameToBehavior = (columnSelector collect {
        case cn:CollectionColumnName => cn.columnName -> cn.collectionBehavior
      }).toMap

    val setNonCounterColumnsClause = for {
      colDef <- nonCounterColumns
      name = colDef.columnName
      collectionBehavior = nameToBehavior.get(name)
      quotedName = quote(name)
    } yield collectionBehavior match {
        case Some(CollectionAppend)           => s"$quotedName = $quotedName + :$quotedName"
        case Some(CollectionPrepend)          => s"$quotedName = :$quotedName + $quotedName"
        case Some(CollectionRemove)           => s"$quotedName = $quotedName - :$quotedName"
        case Some(CollectionOverwrite) | None => s"$quotedName = :$quotedName"
      }

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val setCounterColumnsClause = quotedColumnNames(counterColumns).map(c => s"$c = $c + :$c")
    val setClause = (setNonCounterColumnsClause ++ setCounterColumnsClause).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    // Counter updates don't support USING TIMESTAMP or USING TTL.
    // For all other UPDATEs, assign per-statement timestamps to ensure that multiple
    // mutations to the same list within a batch get distinct timestamps. Without this,
    // Scylla drops all but one list mutation when they share the same timestamp.
    // See: https://github.com/scylladb/spark-scylladb-connector/issues/26
    // Note: TTL is now also applied to UPDATE statements when writeConf.ttl is set.
    // Previously TTL was only included in INSERT statements.
    val usingClause = if (isCounterUpdate) {
      ""
    } else {
      val autoTs = if (usesAutoTimestampForUpdate) {
        TableWriter.checkAutoTimestampCollisions(columnNames, writeConf)
        Some(s"TIMESTAMP :${TableWriter.AutoTimestampParam}")
      } else {
        None
      }
      val parts = List(ttlSpec, timestampSpec.orElse(autoTs)).flatten
      if (parts.nonEmpty) s" USING ${parts.mkString(" AND ")}" else ""
    }

    s"UPDATE ${quote(keyspaceName)}.${quote(tableName)}${usingClause} SET $setClause WHERE $whereClause"
  }

  private lazy val timestampSpec: Option[String] = {
    writeConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"TIMESTAMP ${optionBindMarker(placeholder)}")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }
  }

  private lazy val ttlSpec: Option[String] = {
    writeConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"TTL ${optionBindMarker(placeholder)}")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }
  }

  private def optionBindMarker(placeholder: String): String =
    s":${CqlIdentifier.fromInternal(placeholder).asCql(true)}"

  private val isCounterUpdate =
    tableDef.columns.exists(_.isCounterColumn)

  private val containsCollectionBehaviors =
    columnSelector.exists(_.isInstanceOf[CollectionColumnName])

  private lazy val usesAutoTimestampForUpdate: Boolean =
    !isCounterUpdate && timestampSpec.isEmpty

  private[connector] lazy val isIdempotent: Boolean = {
    // DELETEs are inherently idempotent
    if (isDelete) {
      true
    //All counter operations are not Idempotent
    } else if (columns.exists(_.isCounterColumn)) {
       false
    } else {
      columnSelector.forall {
        //Any appends or prepends to a list are non-idempotent
        case cn: CollectionColumnName =>
          val name = cn.columnName
          val behavior = cn.collectionBehavior
          val isNotList = !tableDef.columnByName(name).columnType.isInstanceOf[ListType[_]]
          behavior match {
            case CollectionPrepend => isNotList
            case CollectionAppend => isNotList
            case _ => true
          }
       //All other operations on regular columns are idempotent
       case regularColumn: ColumnRef => true
      }
    }
  }

  private def prepareStatement(
      queryTemplate:String,
      session: CqlSession,
      idempotent: Boolean): PreparedStatement = {
    try {
      val stmt = SimpleStatement.newInstance(queryTemplate)
        .setIdempotent(idempotent)
        .setConsistencyLevel(writeConf.consistencyLevel)
      session.prepare(stmt)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $queryTemplate: " + t.getMessage, t)
    }
  }

  def batchRoutingKey(session: CqlSession): RichBoundStatementWrapper => Any = {
    writeConf.batchGroupingKey match {
      case BatchGroupingKey.None => (_: RichBoundStatementWrapper) => 0

      case BatchGroupingKey.ReplicaSet =>
        val tokenMap = session.getMetadata.getTokenMap.orElseThrow(
          new Supplier[IllegalArgumentException] {
            override def get(): IllegalArgumentException = new IllegalArgumentException("TokenMap Metadata Missing")
          })
        (bs: RichBoundStatementWrapper) => tokenMap.getReplicas(keyspaceName, QueryUtils.getRoutingKeyOrError(bs.stmt))

      case BatchGroupingKey.Partition =>
        (bs: RichBoundStatementWrapper) => QueryUtils.getRoutingKeyOrError(bs.stmt)
    }
  }

  /**
    * Main entry point
    * if counter or collection column need to be updated Cql UPDATE command will be used
    * INSERT otherwise
    */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    require(!isDelete, "write() must not be called on a delete-configured TableWriter; use delete() instead")
    val asyncStatementWriter = getAsyncWriter()
    writeInternal(asyncStatementWriter, taskContext, data)
  }

  /**
    * Cql DELETE statement. The writer must be created via [[TableWriter.forDelete]] which
    * eagerly validates and caches the delete query template.
    */
  private[connector] def deleteRows(taskContext: TaskContext, data: Iterator[T]): Unit = {
    val template = cachedDeleteQueryTemplate.getOrElse(
      throw new IllegalStateException(
        "deleteRows() requires a cached delete query template; create the writer with TableWriter.forDelete()"))
    writeInternal(getAsyncWriterInternal(template), taskContext, data)
  }

  /** Compatibility entry point for callers that construct a TableWriter directly
    * and pass delete columns at call time. Prefer [[TableWriter.forDelete]] for
    * new code so DELETE validation happens before the Spark job is submitted. */
  def delete(columns: ColumnSelector)(taskContext: TaskContext, data: Iterator[T]): Unit =
    writeInternal(getAsyncWriterInternal(deleteQueryTemplate(columns), isDeleteStatement = true), taskContext, data)

  def extractTokenRange(partitionId: Int): Iterable[CqlTokenRange[_, _]] =
    partitions.lift(partitionId) match {
      case Some(CassandraPartition(_, _, ranges, _)) => ranges
      case _ => List()
    }

  def getAsyncWriter(): AsyncStatementWriter[T] = {
    require(!isDelete, "getAsyncWriter() must not be called on a delete-configured TableWriter; use delete() instead")
    if (isCounterUpdate || containsCollectionBehaviors) {
      getAsyncWriterInternal(
        queryTemplateUsingUpdate,
        autoTimestampParam = if (usesAutoTimestampForUpdate) Some(TableWriter.AutoTimestampParam) else None)
    }
    else {
      getAsyncWriterInternal(queryTemplateUsingInsert)
    }
  }

  private def getAsyncWriterInternal(
      queryTemplate: String,
      autoTimestampParam: Option[String] = None,
      isDeleteStatement: Boolean = false): AsyncStatementWriter[T] = {
    connector.withSessionDo { session =>
      val protocolVersion = session.getContext.getProtocolVersion
      val deleteStatement = isDelete || isDeleteStatement
      val statementIdempotent = if (deleteStatement) true else isIdempotent
      val stmt = prepareStatement(queryTemplate, session,
        idempotent = statementIdempotent)
      val batchType = if (isCounterUpdate) DefaultBatchType.COUNTER else DefaultBatchType.UNLOGGED

      val boundStmtBuilder = new BoundStatementBuilder(
        rowWriter,
        stmt,
        protocolVersion = protocolVersion,
        ignoreNulls = writeConf.ignoreNulls,
        autoTimestampParam = autoTimestampParam)

      val batchStmtBuilder = new BatchStatementBuilder(
        batchType,
        writeConf.consistencyLevel,
        statementIdempotent)
      val batchKeyGenerator = batchRoutingKey(session)
      val batchBuilder = new GroupingBatchBuilderBase(boundStmtBuilder, batchStmtBuilder, batchKeyGenerator,
        writeConf.batchSize, writeConf.batchGroupingBufferSize)

      val maybeRateLimit: RichStatement => Unit = writeConf.throughputMiBPS match {
        case Some(throughput) =>
          val rateLimiter = new RateLimiter(
            (throughput * 1024 * 1024).toLong,
            1024 * 1024)
          (stmt: RichStatement) => rateLimiter.maybeSleep(stmt.bytesCount)
        case None =>
          (stmt: RichStatement) => ()
      }

      AsyncStatementWriter(connector, writeConf, tableDef, stmt, batchBuilder, maybeRateLimit)
    }
  }

  private def writeInternal(asyncStatementWriter: AsyncStatementWriter[T], taskContext: TaskContext, data: Iterator[T]): Unit = {
    val updater = OutputMetricsUpdater(taskContext, writeConf)
    val tokenRanges = extractTokenRange(taskContext.partitionId())
    logInfo(s"Writing ranges: ${tokenRanges}")

    val metricMonitoringWriter = asyncStatementWriter.copy(
        successHandler = Some(updater.batchFinished(success = true, _, _, _)),
        failureHandler = Some(updater.batchFinished(success = false, _, _, _)))

    val rowIterator = new CountingIterator(data)

    logDebug(s"Writing data partition to $keyspaceName.$tableName in batches of ${writeConf.batchSize}.")

    for (stmtToWrite <- rowIterator) {
      metricMonitoringWriter.write(stmtToWrite)
    }

    metricMonitoringWriter.close()

    val duration = updater.finish() / 1000000000d
    logInfo(f"Wrote ${rowIterator.count} rows to $keyspaceName.$tableName in $duration%.3f s.")

    tokenRangeAcc.foreach(_.add(tokenRanges.toSet))
    logInfo("Added token ranges to accumulator")
  }
}

case class AsyncStatementWriter[T](
  connector: CassandraConnector,
  writeConf: WriteConf,
  tableDef: TableDef,
  preparedStatement: PreparedStatement,
  groupingBatchBuilderBase: GroupingBatchBuilderBase[T],
  maybeRateLimit: RichStatement => Unit,
  successHandler: Option[Handler[RichStatement]] = None,
  failureHandler: Option[Handler[RichStatement]] = None)
  extends Closeable
    with Logging {

  //Don't grab a connection or queryExecutor unless we are using this statement writer
  private lazy val session: CqlSession = connector.openSession()
  private val keyspaceName: String = tableDef.keyspaceName
  private val tableName: String = tableDef.tableName

  private lazy val queryExecutor = new QueryExecutor(
    session, writeConf.parallelismLevel, successHandler, failureHandler,
    maxRetries = connector.conf.queryRetryMaxRetries)

  def write(record: T): Unit= {
    groupingBatchBuilderBase.batchRecord(record).foreach{ stmt =>
      maybeRateLimit(stmt)
      queryExecutor.executeAsync(stmt.executeAs(writeConf.executeAs))
    }
  }

  override def close(): Unit = {
    for (statement <- groupingBatchBuilderBase.finish()) {
      maybeRateLimit(statement)
      queryExecutor.executeAsync(statement.executeAs(writeConf.executeAs))
    }

    queryExecutor.waitForCurrentlyExecutingTasks()
    queryExecutor.getLatestException().map {
      case exception =>
        throw new IOException(
          s"""Failed to write statements to $keyspaceName.$tableName. The
             |latest exception was
             |  ${exception.getMessage}
             |
             |Please check the executor logs for more exceptions and information
             """.stripMargin)
    }
    session.close()
  }
}

object TableWriter {

  /** Name of the auto-generated timestamp bind parameter added to UPDATE statements. */
  private[writer] val AutoTimestampParam = "connector_autots"

  private[writer] def checkAutoTimestampCollisions(columnNames: Seq[String], writeConf: WriteConf): Unit = {
    if (columnNames.contains(AutoTimestampParam))
      throw new IllegalArgumentException(
        s"Selected column '$AutoTimestampParam' conflicts with internal auto-timestamp parameter. " +
        "Rename the column or use an explicit timestamp via WriteConf.")
    checkOptionPlaceholderCollisions(writeConf)
  }

  private def checkOptionPlaceholderCollisions(writeConf: WriteConf): Unit = {
    if (writeConf.optionPlaceholders.contains(AutoTimestampParam))
      throw new IllegalArgumentException(
        s"Per-row placeholder name '$AutoTimestampParam' conflicts with internal auto-timestamp parameter.")
  }

  private def checkMissingColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val allColumnNames = table.columns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Column(s) not found: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingPrimaryKeyColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val primaryKeyColumnNames = table.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    if (missingPrimaryKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
  }

  private def checkMissingPartitionKeyColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val partitionKeyColumnNames = table.partitionKey.map(_.columnName)
    val missingPartitionKeyColumns = partitionKeyColumnNames.toSet -- columnNames
    if (missingPartitionKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some partition key columns are missing in RDD or have not been selected: ${missingPartitionKeyColumns.mkString(", ")}")
  }

  private def onlyPartitionKeyAndStatic(table: TableDef, columnNames: Seq[String]): Boolean = {
    val nonPartitionKeyColumnNames = columnNames.toSet -- table.partitionKey.map(_.columnName)
    val nonPartitionKeyColumnRefs = table
      .allColumns
      .filter(columnDef => nonPartitionKeyColumnNames.contains(columnDef.columnName))
    nonPartitionKeyColumnRefs.forall( columnDef => columnDef.columnRole == StaticColumn)
  }

  /**
   * Check whether a collection behavior is being applied to a non collection column
   * Check whether prepend is used on any Sets or Maps
   * Check whether remove is used on Maps
   */
  private def checkCollectionBehaviors(table: TableDef, columnRefs: IndexedSeq[ColumnRef]): Unit = {
    val tableCollectionColumns = table.columns.filter(cd => cd.isCollection)
    val tableCollectionColumnNames = tableCollectionColumns.map(_.columnName)
    val tableListColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: ListType[_]) => name }

    val tableMapColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: MapType[_, _]) => name }

    val refsWithCollectionBehavior = columnRefs collect {
      case columnName: CollectionColumnName => columnName
    }

    val collectionBehaviorColumnNames = refsWithCollectionBehavior.map(_.columnName)

    //Check for non-collection columns with a collection Behavior
    val collectionBehaviorNormalColumn =
      collectionBehaviorColumnNames.toSet -- tableCollectionColumnNames.toSet

    if (collectionBehaviorNormalColumn.nonEmpty)
      throw new IllegalArgumentException(
        s"""Collection behaviors (add/remove/append/prepend) are only allowed on collection columns.
           |Normal Columns with illegal behavior: ${collectionBehaviorNormalColumn.mkString}"""
          .stripMargin
      )

    //Check that prepend is used only on lists
    val prependBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionPrepend)
      .map(_.columnName)
    val prependOnNonList = prependBehaviorColumnNames.toSet -- tableListColumnNames.toSet

    if (prependOnNonList.nonEmpty)
      throw new IllegalArgumentException(
        s"""The prepend collection behavior only applies to Lists. Prepend used on:
           |${prependOnNonList.mkString}""".stripMargin
      )

    //Check that remove is not used on Maps

    val removeBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionRemove)
      .map(_.columnName)

    val removeOnMap = removeBehaviorColumnNames.toSet & tableMapColumnNames.toSet

    if (removeOnMap.nonEmpty)
      throw new IllegalArgumentException(
        s"The remove operation is currently not supported for Maps. Remove used on: ${removeOnMap
          .mkString}"
      )
  }

  private def checkColumns(
      table: TableDef,
      columnRefs: IndexedSeq[ColumnRef],
      partitionKeyOnly: Boolean,
      isDelete: Boolean) = {
    val columnNames = columnRefs.map(_.columnName)
    checkMissingColumns(table, columnNames)
    if (partitionKeyOnly) {
      // For Deletes we only need a partition Key for a valid delete statement
      checkMissingPartitionKeyColumns(table, columnNames)
    }
    else if (isDelete) {
      // Non-static column deletes require a full primary key. The write-side static-column
      // exception below does not apply because delete columns are passed separately.
      checkMissingPrimaryKeyColumns(table, columnNames)
    }
    else if (onlyPartitionKeyAndStatic(table, columnNames)) {
      // Cassandra only requires a Partition Key Column on insert if all other columns are Static
      checkMissingPartitionKeyColumns(table, columnNames)
    }
    else {
      // For all other normal Cassandra writes we require the full primary key to be present
      checkMissingPrimaryKeyColumns(table, columnNames)
    }
    checkCollectionBehaviors(table, columnRefs)
  }

  private[writer] def isColumnDelete(table: TableDef, deleteColumns: ColumnSelector): Boolean = deleteColumns match {
    case c: SomeColumns =>
      c.columns.exists(col => table.columnByName.get(col.columnName).exists { colDef =>
        !colDef.isPrimaryKeyColumn && !colDef.isStatic
      })
    case AllColumns =>
      table.regularColumns.exists(col => !col.isStatic)
    case _ => false
  }

  private[writer] def validateDeleteSelectors(
      tableDef: TableDef,
      deleteColumns: ColumnSelector,
      keyColumns: ColumnSelector): Unit = {
    keyColumns match {
      case AllColumns if tableDef.regularColumns.nonEmpty =>
        throw new IllegalArgumentException(
          "AllColumns cannot be used as keyColumns for DELETE when the table has non-primary-key columns; " +
          "use PrimaryKeyColumns or SomeColumns(...)")
      case _ =>
    }
    deleteColumns match {
      case PrimaryKeyColumns | PartitionKeyColumns =>
        throw new IllegalArgumentException(
          s"$deleteColumns cannot be used as deleteColumns for DELETE; " +
          "use SomeColumns(...) to delete specific columns or SomeColumns() for full-row delete")
      case _ =>
    }
  }

  private[writer] def nonPrimaryKeyOptionPlaceholders(tableDef: TableDef, writeConf: WriteConf): Set[String] =
    writeConf.optionPlaceholders.filterNot { name =>
      tableDef.columnByName.get(name).exists(_.isPrimaryKeyColumn)
    }.toSet

  private class FallbackRowWriter[T](primary: RowWriter[T], fallback: RowWriter[T]) extends RowWriter[T] {

    override val columnNames: scala.collection.immutable.Seq[String] = primary.columnNames

    private val fallbackColumnNames = fallback.columnNames.toIndexedSeq
    private val fallbackBuffer = Array.ofDim[Any](fallbackColumnNames.size)
    private val primaryIndexByName = columnNames.zipWithIndex.toMap
    private val copyPlan = fallbackColumnNames.zipWithIndex.map { case (columnName, fallbackIndex) =>
      fallbackIndex -> primaryIndexByName(columnName)
    }

    override def readColumnValues(data: T, buffer: Array[Any]): Unit = {
      try {
        primary.readColumnValues(data, buffer)
      } catch {
        case NonFatal(primaryFailure) =>
          try {
            fallback.readColumnValues(data, fallbackBuffer)
            copyPlan.foreach { case (fallbackIndex, primaryIndex) =>
              buffer(primaryIndex) = fallbackBuffer(fallbackIndex)
            }
          } catch {
            case NonFatal(_) => throw primaryFailure
          }
      }
    }
  }

  private def rowWriterWith[T](
      rowWriterFactory: RowWriterFactory[T],
      tableDefWithMeta: TableDef,
      optionColumns: Seq[ColumnDef],
      selectedColumns: scala.collection.immutable.IndexedSeq[ColumnRef],
      bindOnlyColumnNames: Set[String]): RowWriter[T] = {
    // Bind-only option placeholders should be extracted with TTL/TIMESTAMP types,
    // even when their names match real table columns.
    val bindOnlyOptionColumnsByName = optionColumns
      .filter(c => bindOnlyColumnNames.contains(c.columnName))
      .map(c => c.columnName -> c)
      .toMap
    val tableDefForRowWriter =
      if (bindOnlyOptionColumnsByName.isEmpty) {
        tableDefWithMeta
      } else {
        tableDefWithMeta.copy(regularColumns = tableDefWithMeta.regularColumns.map { column =>
          bindOnlyOptionColumnsByName.getOrElse(column.columnName, column)
        })
      }
    rowWriterFactory.rowWriter(tableDefForRowWriter, selectedColumns)
  }

  private def rowWriterSelection[T](
      rowWriterFactory: RowWriterFactory[T],
      tableDefWithMeta: TableDef,
      optionColumns: Seq[ColumnDef],
      selectedColumns: scala.collection.immutable.IndexedSeq[ColumnRef],
      syntheticColumnNames: Set[String],
      autoAddedOptionColumnNames: Set[String],
      deleteOptionColumnNames: Set[String],
      retryWithoutIgnoredDeleteTtl: Option[String]):
      (scala.collection.immutable.IndexedSeq[ColumnRef], Set[String], RowWriter[T]) = {

    def build(
        selectedColumns: scala.collection.immutable.IndexedSeq[ColumnRef],
        autoAddedOptionColumnNames: Set[String]) = {
      val bindOnlyColumnNames = syntheticColumnNames ++ autoAddedOptionColumnNames ++ deleteOptionColumnNames
      rowWriterWith(rowWriterFactory, tableDefWithMeta, optionColumns, selectedColumns, bindOnlyColumnNames)
    }

    retryWithoutIgnoredDeleteTtl match {
      case Some(ttlPlaceholder) =>
        // DELETE ignores TTL. Keep the ignored TTL field when it exists to preserve tuple
        // positions, but do not require named row types to define it.
        val selectedWithoutIgnoredTtl = selectedColumns.filterNot(_.columnName == ttlPlaceholder)
        val autoAddedWithoutIgnoredTtl = autoAddedOptionColumnNames - ttlPlaceholder
        try {
          val primary = build(selectedColumns, autoAddedOptionColumnNames)
          val rowWriter = try {
            new FallbackRowWriter(primary, build(selectedWithoutIgnoredTtl, autoAddedWithoutIgnoredTtl))
          } catch {
            case NonFatal(_) => primary
          }
          (selectedColumns, autoAddedOptionColumnNames, rowWriter)
        } catch {
          case NonFatal(original) =>
            try {
              (selectedWithoutIgnoredTtl, autoAddedWithoutIgnoredTtl,
                build(selectedWithoutIgnoredTtl, autoAddedWithoutIgnoredTtl))
            } catch {
              case NonFatal(_) => throw original
            }
        }
      case None =>
        (selectedColumns, autoAddedOptionColumnNames, build(selectedColumns, autoAddedOptionColumnNames))
    }
  }

  /** Creates a [[TableWriter]] configured for DELETE operations.
    * Determines whether this is a column-level or full-row delete, adjusts the
    * [[WriteConf]] (stripping TTL and other INSERT-only options), and constructs
    * the writer with the appropriate key-column validation mode.
    * Eagerly validates the delete query template on the driver to fail fast
    * rather than wasting cluster resources on executor-side validation. */
  private[connector] def forDelete[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      deleteColumns: ColumnSelector,
      keyColumns: ColumnSelector,
      writeConf: WriteConf): TableWriter[T] = {
    val tableDef = tableFromCassandra(connector, keyspaceName, tableName)
    validateDeleteSelectors(tableDef, deleteColumns, keyColumns)
    // Column delete requires full primary key; partition key is enough otherwise.
    val columnDelete = isColumnDelete(tableDef, deleteColumns)
    val deleteConf = writeConf.forDelete
    val writer = TableWriter[T](connector, tableDef, keyColumns, deleteConf,
      partitionKeyOnly = !columnDelete, partitions = Array(), tokenRangeAcc = None, isDelete = true)
    // Eagerly compute and cache the delete query template on the driver side.
    // This triggers all validation (column existence, PK checks, collection checks)
    // before the Spark job is submitted, avoiding wasted cluster resources.
    writer.cachedDeleteQueryTemplate = Some(writer.deleteQueryTemplate(deleteColumns))
    writer
  }

  /** Columns that cannot actually be written to because they represent virtual endpoints
    */
  private val InternalColumns = Set("solr_query")

  def apply[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      columnNames: ColumnSelector,
      writeConf: WriteConf,
      checkPartitionKey: Boolean = false,
      partitions: Array[Partition] = Array(),
      tokenRangeAcc: Option[TokenRangeAccumulator] = None): TableWriter[T] = {

    val tableDef = tableFromCassandra(connector, keyspaceName, tableName)
    TableWriter(connector, tableDef, columnNames, writeConf, checkPartitionKey, partitions, tokenRangeAcc)
  }

  def apply[T : RowWriterFactory](
       connector: CassandraConnector,
       tableDef: TableDef,
       columnNames: ColumnSelector,
       writeConf: WriteConf,
       checkPartitionKey: Boolean,
       partitions: Array[Partition],
       tokenRangeAcc: Option[TokenRangeAccumulator]): TableWriter[T] = {

    TableWriter(connector, tableDef, columnNames, writeConf, checkPartitionKey, partitions, tokenRangeAcc, isDelete = false)
  }

  def apply[T : RowWriterFactory](
       connector: CassandraConnector,
       tableDef: TableDef,
       columnNames: ColumnSelector,
       writeConf: WriteConf,
       partitionKeyOnly: Boolean,
       partitions: Array[Partition],
       tokenRangeAcc: Option[TokenRangeAccumulator],
       isDelete: Boolean): TableWriter[T] = {

    val optionColumns = writeConf.optionsAsColumns(tableDef.keyspaceName, tableDef.tableName)
    val tableHasCounterColumns = tableDef.columns.exists(_.isCounterColumn)
    val candidateAutoSelectedOptionColumns = if (isDelete) {
      (writeConf.ttl, writeConf.timestamp) match {
        case (TTLOption(PerRowWriteOptionValue(_)), TimestampOption(PerRowWriteOptionValue(_))) =>
          // DELETE never binds TTL, but when a per-row timestamp follows it in a tuple,
          // keep the ignored TTL placeholder selected so tuple positions do not shift.
          optionColumns
        case (TTLOption(PerRowWriteOptionValue(ttlPlaceholder)), _) =>
          // TTL-only DELETEs should not require an ignored placeholder when omitted.
          optionColumns.filterNot(_.columnName == ttlPlaceholder)
        case _ => optionColumns
      }
    } else if (tableHasCounterColumns) {
      // Counter UPDATE statements do not support TTL or TIMESTAMP, so per-row
      // placeholders must not be required unless the caller selected them explicitly.
      Seq.empty
    } else {
      optionColumns
    }
    val existingColumnNames = tableDef.columns.map(_.columnName).toSet
    val newOptionColumns = optionColumns.filterNot(c => existingColumnNames.contains(c.columnName))
    val syntheticColumnNames = newOptionColumns.map(_.columnName).toSet
    val tablDefWithMeta = tableDef.copy(regularColumns = tableDef.regularColumns ++ newOptionColumns)

    val candidateAutoSelectedOptionColumnNames = candidateAutoSelectedOptionColumns.map(_.columnName).toSet
    val selectableOptionColumns = columnNames match {
      case AllColumns => newOptionColumns.filter(c => candidateAutoSelectedOptionColumnNames.contains(c.columnName))
      case _ => newOptionColumns
    }
    val tableDefForSelection = tableDef.copy(regularColumns = tableDef.regularColumns ++ selectableOptionColumns)

    val baseSelectedColumns = columnNames
      .selectFrom(tableDefForSelection)
      .filter(col => !InternalColumns.contains(col.columnName))
    val selectedOnlyPrimaryKeyColumns = baseSelectedColumns.nonEmpty && baseSelectedColumns.forall { col =>
      tableDef.columnByName.get(col.columnName).exists(_.isPrimaryKeyColumn)
    }
    val autoSelectedOptionColumns = writeConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder))
          if !isDelete && selectedOnlyPrimaryKeyColumns && syntheticColumnNames.contains(placeholder) =>
        candidateAutoSelectedOptionColumns.filterNot(_.columnName == placeholder)
      case _ => candidateAutoSelectedOptionColumns
    }
    // Ensure per-row option placeholder columns (e.g. per-row timestamp/TTL) are included
    // when the selected column set did not include them.
    val selectedColumnNames = baseSelectedColumns.map(_.columnName).toSet
    val missingOptionColumns = autoSelectedOptionColumns.filterNot(c => selectedColumnNames.contains(c.columnName))
    val selectedColumns = baseSelectedColumns ++ missingOptionColumns.map(c => ColumnName(c.columnName))

    val autoAddedOptionColumnNames = missingOptionColumns.map(_.columnName).toSet
    val deleteOptionColumnNames =
      if (isDelete) nonPrimaryKeyOptionPlaceholders(tableDef, writeConf) else Set.empty[String]
    val bindOnlyColumnNames = syntheticColumnNames ++ autoAddedOptionColumnNames ++ deleteOptionColumnNames
    val selectedColumnsForValidation = selectedColumns.filterNot(c => bindOnlyColumnNames.contains(c.columnName))
    checkColumns(tableDef, selectedColumnsForValidation, partitionKeyOnly, isDelete)

    val retryWithoutIgnoredDeleteTtl = (writeConf.ttl, writeConf.timestamp) match {
      case (TTLOption(PerRowWriteOptionValue(ttlPlaceholder)), TimestampOption(PerRowWriteOptionValue(_)))
          if isDelete && autoAddedOptionColumnNames.contains(ttlPlaceholder) =>
        Some(ttlPlaceholder)
      case _ => None
    }

    val (finalSelectedColumns, finalAutoAddedOptionColumnNames, rowWriter) = rowWriterSelection(
      implicitly[RowWriterFactory[T]], tablDefWithMeta, optionColumns, selectedColumns, syntheticColumnNames,
      autoAddedOptionColumnNames, deleteOptionColumnNames, retryWithoutIgnoredDeleteTtl)

    new TableWriter[T](connector, tablDefWithMeta, finalSelectedColumns, rowWriter, writeConf, partitions, tokenRangeAcc, isDelete,
      syntheticColumnNames = syntheticColumnNames,
      autoAddedOptionColumnNames = finalAutoAddedOptionColumnNames)
  }
}
