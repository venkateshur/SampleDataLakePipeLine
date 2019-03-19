package com.org.datapipeline.model.init

import com.org.datapipeline.model.process.Process
import com.org.datapipeline.util.Driver
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object InitDataPipeLine {

  def main (args: Array[String]): Unit = {

    val hiveMetastoreUri = args(0)
    val hiveWarehouse = args(1)

    val sparkSession = initSparkSession(hiveMetastoreUri, hiveWarehouse)
    val driver = new Driver
    driver.loadTablePropertits("path", sparkSession)

    val loadStagingTable = sparkSession.sql("""select * from db.stage""").persist(MEMORY_AND_DISK)
    if(loadStagingTable.take(1).nonEmpty) {
      Process.takesnapShotBackUp()
      Process.
      Process.UpdateHistory()
      Process.UpdateTarget()
    }


  }

  def initSparkSession(hiveMetastoreUri:  String, hiveWarehouseDir: String) = {
    SparkSession.
      builder().
      appName("dataPipeLine").
      config("spark.sql.warehouse.dir",  hiveWarehouseDir).
      config("hive.metastore.uris",  hiveMetastoreUri).enableHiveSupport().getOrCreate()
  }



}
