package com.org.datapipeline.model.write

import org.apache.spark.sql.{DataFrame, SparkSession}

object Write {

  def loadToHive(tableName: String)(df: DataFrame) =  df.write.mode("append").saveAsTable(tableName)

}
