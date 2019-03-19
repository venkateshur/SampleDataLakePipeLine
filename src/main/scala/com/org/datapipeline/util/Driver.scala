package com.org.datapipeline.util
import com.org.datapipeline.{DataPipeLineProperties, TableInfo}
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

case class TableProperties(tableNames: List[(String, String)])

class Driver {

  var dataPipeLineProps: DataPipeLineProperties = DataPipeLineProperties()

  def initializeDriver(args: Array[String])(sparkSession: SparkSession)  {
    loadTablePropertits(args(0))(sparkSession)
  }

  def loadTablePropertits(propConfigPath: String)(sparkSession: SparkSession) {
    val jsonMap = JSON.parseFull(sparkSession.read.textFile(propConfigPath).collect().mkString(" ")).get.asInstanceOf[Map[String, Any]]
    val jsonProps = jsonMap("dataPipeLineProperties").asInstanceOf[Map[String,Any]]

    dataPipeLineProps = dataPipeLineProps.copy(
      tables = jsonProps.get("tableNames").asInstanceOf[Option[List[TableInfo]]])

  }

}
