package com.org.datapipeline

import com.org.datapipeline.util.Driver
import org.apache.spark.sql.SparkSession

import com.org.datapipeline.model.process.Process

import scala.util.{Try, Success, Failure}

object starter extends App{

  val sparkSession = createSparkSession(args(1))
  val driver: Driver = initializeDriver(args)(sparkSession)


  Try{
    Process.invokeDataLoading(driver)(sparkSession)
  } match {
    case Success(_) => sparkSession.stop()
    case Failure(e) =>  sparkSession.stop(); throw e
  }

  def createSparkSession(hiveMetaStore: String): SparkSession = {
    SparkSession
      .builder()
      .appName("Data Pipe Line")
      .config("hive.metastore.uris", hiveMetaStore)
      .enableHiveSupport()
      .getOrCreate()
  }

  def initializeDriver(args: Array[String])(sparkSession: SparkSession): Driver = {
    val driver = new Driver
    driver.loadTablePropertits(args(0))(sparkSession)
    driver
  }
}
