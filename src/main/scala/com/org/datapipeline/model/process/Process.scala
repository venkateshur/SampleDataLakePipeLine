package com.org.datapipeline.model.process

import com.org.datapipeline.model.read.Read
import com.org.datapipeline.model.write.Write
import com.org.datapipeline.util.Driver
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Process {

  def invokeDataLoading(driver: Driver)(sparkSession: SparkSession): Unit = {
    driver.dataPipeLineProps.tables.get.foreach{ tables =>

      val baseTableDf = loadTable(tables.tableInfo._1.baseTable)(sparkSession)
      val backUpTableDf = loadTable(tables.tableInfo._2.backUpTable)(sparkSession)
      val historyTableDf = loadTable(tables.tableInfo._2.historyTable)(sparkSession)

      //val totalInsers = findInsers(baseTableDf,backUpTableDf).withColumn("status", lit("inser"))
      val totalDeletes = findDeletes(baseTableDf, backUpTableDf).withColumn("status", lit("delete"))
      val totalUpdates = findUpdates(backUpTableDf, backUpTableDf).withColumn("status", lit("update"))
      val maxRowNumber = historyTableDf.agg(Map("customer_seq_id" -> "max")).take(1).mkString.toLong
      val prepareHistoryData = prepareHistoryInfo(totalUpdates, totalDeletes)(maxRowNumber)(sparkSession)
      val preparstas = sparkSession.emptyDataFrame
      Write.loadToHive(tables.tableInfo._2.historyTable)(prepareHistoryData)
      Write.loadToHive(tables.tableInfo._2.statsTable)(preparstas)
    }

  }

  def loadTable(tableName: String)(sparkSession: SparkSession): DataFrame =
    Read.loadFromHive(tableName)(sparkSession)


  def findInsers(baseTableDf: DataFrame, backupTableDf: DataFrame) =
    baseTableDf.join(backupTableDf, baseTableDf("customer_id") === backupTableDf("customer_id"), "left_anti")


  def findDeletes(baseTableDf: DataFrame, backupTableDf: DataFrame) =
    baseTableDf.join(backupTableDf, baseTableDf("customer_id") === backupTableDf("customer_id"), "left_anti")


  def findUpdates(baseTableDf: DataFrame, backupTableDf: DataFrame) =
    baseTableDf.join(backupTableDf, baseTableDf("customer_id") === backupTableDf("customer_id"), "left_anti")


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

}
