package com.org.datapipeline.model.process

import com.org.datapipeline.model.read.Read
import com.org.datapipeline.model.write.Write
import com.org.datapipeline.util.Driver
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

case class Etl_stats(Pre_Count: Long, Delta_count: Long, Post_counts: Long, Updated_counts: Long, Deleted_counts: Long, last_modified_by: String)

object Process {

  def invokeDataLoading(driver: Driver)(sparkSession: SparkSession): Unit = {
    driver.dataPipeLineProps.tables.get.foreach{ tables =>

      val baseTableDf = loadTable(tables.tableInfo._1.baseTable)(sparkSession).persist(MEMORY_AND_DISK)
      val backUpTableDf = loadTable(tables.tableInfo._2.backUpTable)(sparkSession).persist(MEMORY_AND_DISK)
      val historyTableDf = loadTable(tables.tableInfo._2.historyTable)(sparkSession).persist(MEMORY_AND_DISK)

      val totalDeletes = findDeletes(baseTableDf, backUpTableDf).withColumn("status", lit("delete")).persist(MEMORY_AND_DISK)

      val totalUpdates = findUpdates(backUpTableDf, backUpTableDf).withColumn("status", lit("update")).persist(MEMORY_AND_DISK)

      val totalInserts = findInserts(baseTableDf, backUpTableDf).persist(MEMORY_AND_DISK)

      val maxRowNumber = historyTableDf.agg(Map("customer_seq_id" -> "max")).take(1).mkString.toLong

      val prepareHistoryData = prepareHistoryInfo(totalUpdates, totalDeletes)(maxRowNumber)(sparkSession)


      val prepareStas = prepareStatsInfo(sparkSession, backUpTableDf, backUpTableDf, totalUpdates, totalDeletes, totalInserts)
      Write.loadToHive(tables.tableInfo._2.historyTable)(prepareHistoryData)
      sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      sparkSession.sql("insert into table dpl.ETL_STATS select * from ETL_STATS")
      //Write.loadToHive(tables.tableInfo._2.statsTable)(prepareStas)
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


  def prepareHistoryInfo(totalUpdates: DataFrame, totalDeletes: DataFrame)(prevRowNumber: Long)(sparkSession: SparkSession) = {
    val unionDf = totalUpdates.union(totalDeletes)

    sparkSession.createDataFrame(unionDf.rdd.zipWithIndex.map(line => Row.fromSeq(Seq(line._2 + 1 + prevRowNumber, line._1.toSeq))),
    StructType(Array(StructField("customer_seq_id", LongType, false)) ++ unionDf.schema.fields))
      .withColumn("START_DATE", current_timestamp)
      .withColumn("END_DATE", current_timestamp)
      .withColumn("last_modified_dt", current_timestamp)
      .select(col("customer_seq_Id"), col("customer_id"), col("customer_fname"),
              col("customer_lname"), col("customer_email"), col("customer_password"),
              col("customer_street"), col("customer_city"), col("customer_zipcode"),
              col("customer_state"), col("status"), col("START_DATE"),
              col("END_DATE"), col("last_modified_dt"))
  }

  def prepareStatsInfo(spark: SparkSession, baseTableDf: DataFrame, backUpTableDf: DataFrame, totalUpdates: DataFrame, totalDeletes: DataFrame, totalInserts: DataFrame): DataFrame = {
    val etl_stats_pre_count = backUpTableDf.count
    val etl_stats_pos_count = baseTableDf.count
    val etl_stats_updates = totalUpdates.count
    val etl_stats_deletes = totalDeletes.count
    val etl_stats_inserts = totalInserts.count
    val etl_stats_delta = etl_stats_updates + etl_stats_deletes + etl_stats_inserts
    val data = spark.createDataFrame(Seq(Etl_stats(etl_stats_pre_count,etl_stats_pos_count,etl_stats_updates,etl_stats_deletes,etl_stats_delta,"ETL_USER"))).toDF
    val data_DF = data.withColumn("START_DATE",current_timestamp).withColumn("END_DATE",current_timestamp).withColumn("last_modified_dt",current_timestamp).withColumn("job_name",lit("CUSTOMER_TABLE"))
    val prepareetlstats = spark.createDataFrame(data_DF.rdd.zipWithIndex.map(line => Row.fromSeq(Seq(line._2 + 1 + 0) ++ line._1.toSeq)),StructType(Array(StructField("seq_Id", LongType, false)) ++ data_DF.schema.fields))
    prepareetlstats
  }

}
