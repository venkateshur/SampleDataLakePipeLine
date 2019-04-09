package com.org.datapipeline.model.process

import com.org.datapipeline.model.read.Read
import com.org.datapipeline.model.write.Write
import com.org.datapipeline.util.Driver
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

case class Etl_stats(pre_count: Long, delta_count: Long, post_counts: Long, inserted_counts: Long,updated_counts: Long, deleted_counts: Long, last_modified_by: String)

object Process {

  def invokeDataLoading(driver: Driver)(sparkSession: SparkSession) {
    val statsTableDf = loadTable(driver.dataPipeLineProps.statusTable)(sparkSession).persist(MEMORY_AND_DISK)

    driver.dataPipeLineProps.tables.foreach{ tables =>
      val baseTableDf = loadTable(tables.tableInfo._1.baseTable)(sparkSession).persist(MEMORY_AND_DISK)
      val backUpTableDf = loadTable(tables.tableInfo._1.backUpTable)(sparkSession).persist(MEMORY_AND_DISK)
      val historyTableDf = loadTable(tables.tableInfo._1.historyTable)(sparkSession).persist(MEMORY_AND_DISK)
      val keyColumns = tables.tableInfo._2.primaryKeyColumn
      val seqIdColumn = tables.tableInfo._2.sequenceIdColumn

      val totalDeletes = findDeletes(baseTableDf, backUpTableDf, keyColumns).withColumn("status", lit("delete")).persist(MEMORY_AND_DISK)

      val totalUpdates = findUpdates(baseTableDf, backUpTableDf, keyColumns).withColumn("status", lit("update")).persist(MEMORY_AND_DISK)

      val totalInserts = findInserts(baseTableDf, backUpTableDf, keyColumns).persist(MEMORY_AND_DISK)

      val maxRowNumberHist = if(historyTableDf.take(1).isEmpty) 0L else historyTableDf.select(max(s"$seqIdColumn")).take(1)(0).mkString.toLong
      val maxRowNumberStats = if(statsTableDf.take(1).isEmpty) 0L else statsTableDf.select(max(s"seq_Id")).take(1)(0).mkString.toLong

      val brodCastMaxRowNumberHist = sparkSession.sparkContext.broadcast(maxRowNumberHist)
      val brodCastMaxRowNumberStats = sparkSession.sparkContext.broadcast(maxRowNumberStats)

      val prepareHistoryData = prepareHistoryInfo(totalUpdates, totalDeletes, historyTableDf.columns, seqIdColumn)(brodCastMaxRowNumberHist.value)(sparkSession)
      val prepareStas = prepareStatsInfo(sparkSession, baseTableDf, backUpTableDf, totalUpdates, totalDeletes, totalInserts)(brodCastMaxRowNumberStats.value)

      Write.loadToHive(tables.tableInfo._1.historyTable)(prepareHistoryData)(sparkSession)
      Write.loadToHive(driver.dataPipeLineProps.statusTable)(prepareStas)(sparkSession)
    }

  }

  def loadTable(tableName: String)(sparkSession: SparkSession): DataFrame =
    Read.loadFromHive(tableName)(sparkSession)

  def findInserts(baseTableDf: DataFrame, backupTableDf: DataFrame, keyColumns: Array[String]): DataFrame = {
    val joinExprForMatch = keyColumns.map{col => baseTableDf(col) === backupTableDf(col)}.reduce(_&&_)
    baseTableDf.join(backupTableDf, joinExprForMatch, "left_anti")
  }

  def findDeletes(baseTableDf: DataFrame, backupTableDf: DataFrame, keyColumns: Array[String]):DataFrame = {
    val joinExprForMatch = keyColumns.map{col => backupTableDf(col) === baseTableDf(col)}.reduce(_&&_)
    backupTableDf.join(baseTableDf, joinExprForMatch, "left_anti")
  }

  def findUpdates(baseTableDf: DataFrame, backupTableDf: DataFrame, keyColumns: Array[String]): DataFrame = {
    val joinExprForMatch = keyColumns.map{col => baseTableDf(col) === backupTableDf(col)}.reduce(_&&_)
    val joinExprForUpdates = baseTableDf.columns.map{col => baseTableDf(col) =!= backupTableDf(col)}.reduce(_||_)

    baseTableDf.join(backupTableDf, joinExprForMatch && joinExprForUpdates, "left_semi")
  }

  def prepareHistoryInfo(totalUpdates: DataFrame, totalDeletes: DataFrame, reqColumns: Array[String], seqIdColumn: String)(prevRowNumber: Long)(sparkSession: SparkSession): DataFrame = {
    val unionDf = totalUpdates.union(totalDeletes)

    val addSeq = sparkSession.createDataFrame(unionDf.rdd.zipWithIndex.map(line => Row.fromSeq(Seq(line._2 + 1.toLong + prevRowNumber) ++ line._1.toSeq)),
      StructType(Array(StructField(s"$seqIdColumn", LongType, false)) ++ unionDf.schema.fields))

    addSeq.withColumn("start_date", current_timestamp)
      .withColumn("end_date", current_timestamp)
      .withColumn("last_modified_dt", current_timestamp)
      .select(reqColumns.head, reqColumns.tail: _*)
  }

  def prepareStatsInfo(spark: SparkSession, baseTableDf: DataFrame, backUpTableDf: DataFrame, totalUpdates: DataFrame, totalDeletes: DataFrame, totalInserts: DataFrame)(maxRowNumber: Long): DataFrame = {
    val etl_stats_pre_count = backUpTableDf.count
    val etl_stats_pos_count = baseTableDf.count
    val etl_stats_updates = totalUpdates.count
    val etl_stats_deletes = totalDeletes.count
    val etl_stats_inserts = totalInserts.count
    val etl_stats_delta = etl_stats_updates + etl_stats_deletes + etl_stats_inserts
    val data = spark.createDataFrame(Seq(Etl_stats(etl_stats_pre_count,etl_stats_delta,etl_stats_pos_count,etl_stats_inserts,etl_stats_updates,etl_stats_deletes,"ETL_USER"))).toDF
    val data_DF = data.withColumn("start_date",current_timestamp).withColumn("end_date",current_timestamp).withColumn("last_modified_dt",current_timestamp).withColumn("Job_Name",lit("CUSTOMER_TABLE"))
    val addSeq = spark.
      createDataFrame(data_DF.rdd.zipWithIndex.map(line => Row.fromSeq(Seq(line._2 + 1.toLong + maxRowNumber) ++ line._1.toSeq)),StructType(Array(StructField("seq_id", LongType, false)) ++ data_DF.schema.fields))

    addSeq.select(col("seq_id"), col("start_date"), col("end_date"), col("pre_count"), col("delta_count"), col("post_counts"), col("inserted_counts"), col("updated_counts"),
      col("deleted_counts"), col("last_modified_dt"), col("last_modified_by"), col("Job_Name"))
  }

}
