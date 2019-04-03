package com.org.datapipeline.model.write

import org.apache.spark.sql.{DataFrame, SparkSession}

object Write {

  def loadToHive(tableName: String)(df: DataFrame)(sparkSession: SparkSession) {
    val tempTable = df.createOrReplaceTempView(s"$tableName")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql(s"insert into table $tableName select * from $tempTable")
  }
}
