package com.org.datapipeline.model.read

import org.apache.spark.sql.SparkSession

object Read {
  def loadFromHive(tableName: String)(sparkSession: SparkSession) =  sparkSession.read.table(tableName)
}
