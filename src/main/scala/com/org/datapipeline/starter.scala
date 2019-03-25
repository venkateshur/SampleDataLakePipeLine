package com.org.datapipeline

import com.org.datapipeline.util.Driver
import org.apache.spark.sql.SparkSession

import com.org.datapipeline.model.process.Process

import scala.util.{Try, Success, Failure}

object starter extends App{

  val hiveMetaStore = args(1)
  val propsPath = args(0)
  val sparkSession = createSparkSession(hiveMetaStore)
  val driver: Driver = initializeDriver(propsPath)(sparkSession)


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

  def initializeDriver(path: String)(sparkSession: SparkSession): Driver = {
    val driver = new Driver
    driver.loadTablePropertits(args(0))(sparkSession)
    driver
  }
}
