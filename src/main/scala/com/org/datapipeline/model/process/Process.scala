package com.org.datapipeline.model.process

import com.org.datapipeline.model.read.Read
import com.org.datapipeline.model.write.Write
import com.org.datapipeline.util.Driver
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

case class Etl_stats(pre_count: Long, delta_count: Long, post_counts: Long, updated_counts: Long, deleted_counts: Long, last_modified_by: String)

object Process {

  def invokeDataLoading(driver: Driver)(sparkSession: SparkSession) {
    driver.dataPipeLineProps.tables.foreach{ tables =>

      val baseTableDf = loadTable(tables.tableInfo._1.baseTable)(sparkSession).persist(MEMORY_AND_DISK)
      val backUpTableDf = loadTable(tables.tableInfo._2.backUpTable)(sparkSession).persist(MEMORY_AND_DISK)
      val historyTableDf = loadTable(tables.tableInfo._2.historyTable)(sparkSession).persist(MEMORY_AND_DISK)
      val statsTableDf = loadTable(tables.tableInfo._2.statsTable)(sparkSession).persist(MEMORY_AND_DISK)

      val totalDeletes = findDeletes(baseTableDf, backUpTableDf).withColumn("status", lit("delete")).persist(MEMORY_AND_DISK)

      val totalUpdates = findUpdates(backUpTableDf, backUpTableDf).withColumn("status", lit("update")).persist(MEMORY_AND_DISK)

      val totalInserts = findInserts(baseTableDf, backUpTableDf).persist(MEMORY_AND_DISK)

      val maxRowNumberHist = sparkSession.sparkContext.broadcast(historyTableDf.select(max("customer_seq_id")).take(1)(0).mkString.toLong)
      val maxRowNumberStats = sparkSession.sparkContext.broadcast(statsTableDf.select(max("seq_Id")).take(1)(0).mkString.toLong)

      val prepareHistoryData = prepareHistoryInfo(totalUpdates, totalDeletes)(maxRowNumberHist.value)(sparkSession)
      val prepareStas = prepareStatsInfo(sparkSession, backUpTableDf, backUpTableDf, totalUpdates, totalDeletes, totalInserts)(maxRowNumberStats.value)
      println(("start loading:..........................................................................................................................."))

      Write.loadToHive(tables.tableInfo._2.historyTable)(prepareHistoryData)(sparkSession)
      Write.loadToHive(tables.tableInfo._2.statsTable)(prepareStas)(sparkSession)
    }

  }

  def loadTable(tableName: String)(sparkSession: SparkSession): DataFrame =
    Read.loadFromHive(tableName)(sparkSession)


  def findInserts(baseTableDf: DataFrame, backupTableDf: DataFrame): DataFrame =
    baseTableDf.join(backupTableDf, baseTableDf("customer_id") === backupTableDf("customer_id"), "left_anti")


  def findDeletes(baseTableDf: DataFrame, backupTableDf: DataFrame):DataFrame =
    backupTableDf.join(baseTableDf, backupTableDf("customer_id") === baseTableDf("customer_id"), "left_anti")


  def findUpdates(baseTableDf: DataFrame, backupTableDf: DataFrame): DataFrame =
    baseTableDf.join(backupTableDf, baseTableDf("customer_id") === backupTableDf("customer_id") && baseTableDf("customer_fname") =!= backupTableDf("customer_fname"), "left_semi")


  def prepareHistoryInfo(totalUpdates: DataFrame, totalDeletes: DataFrame)(prevRowNumber: Long)(sparkSession: SparkSession): DataFrame = {
    val unionDf = totalUpdates.union(totalDeletes)
    import sparkSession.implicits._

    val addSeq = sparkSession.createDataFrame(unionDf.rdd.zipWithIndex.map(line => Row.fromSeq(Seq(line._2 + 1.toLong + prevRowNumber, line._1.toSeq))),
      StructType(Array(StructField("customer_seq_id_gen", LongType, false)) ++ unionDf.schema.fields))

    addSeq.withColumn("start_date", current_timestamp)
      .withColumn("end_date", current_timestamp)
      .withColumn("last_modified_dt", current_timestamp)
      .select($"customer_seq_id_gen".cast("integer").alias("customer_seq_id"), col("customer_id"), col("customer_fname"),
        col("customer_lname"), col("customer_email"), col("customer_password"),
        col("customer_street"), col("customer_city"), col("customer_state"),col("customer_zipcode"),
        col("status"), col("start_date"),
        col("end_date"), col("last_modified_dt"))
  }

  def prepareStatsInfo(spark: SparkSession, baseTableDf: DataFrame, backUpTableDf: DataFrame, totalUpdates: DataFrame, totalDeletes: DataFrame, totalInserts: DataFrame)(maxRowNumber: Long): DataFrame = {
    val etl_stats_pre_count = backUpTableDf.count
    val etl_stats_pos_count = baseTableDf.count
    val etl_stats_updates = totalUpdates.count
    val etl_stats_deletes = totalDeletes.count
    val etl_stats_inserts = totalInserts.count
    val etl_stats_delta = etl_stats_updates + etl_stats_deletes + etl_stats_inserts
    val data = spark.createDataFrame(Seq(Etl_stats(etl_stats_pre_count,etl_stats_pos_count,etl_stats_updates,etl_stats_deletes,etl_stats_delta,"ETL_USER"))).toDF
    val data_DF = data.withColumn("start_date",current_timestamp).withColumn("end_date",current_timestamp).withColumn("last_modified_dt",current_timestamp).withColumn("Job_Name",lit("CUSTOMER_TABLE"))
    val addSeq = spark.
      createDataFrame(data_DF.rdd.zipWithIndex.map(line => Row.fromSeq(Seq(line._2 + 1.toLong + maxRowNumber) ++ line._1.toSeq)),StructType(Array(StructField("seq_id_gen", LongType, false)) ++ data_DF.schema.fields))

    addSeq.select(col("seq_id_gen").cast("integer").alias("seq_id"), col("start_date"), col("end_date"), col("pre_count"), col("delta_count"), col("post_counts"), col("updated_counts"),
      col("deleted_counts"), col("last_modified_dt"), col("last_modified_by"), col("Job_Name"))
  }

}
