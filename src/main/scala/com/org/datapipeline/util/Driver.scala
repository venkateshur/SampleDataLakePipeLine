package com.org.datapipeline.util
import com.org.datapipeline.{BaseTable, DataPipeLineProperties, OtherTables, TableInfo}
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
    val jsonProps = jsonMap("dataPipeLineProperties").asInstanceOf[Map[String,List[Map[String, List[String]]]]]
    createTableInfo(jsonProps.get("tableInfo").get)
  }

  def createTableInfo(tableData: List[Map[String, List[String]]]) {
    dataPipeLineProps = dataPipeLineProps.copy(
      tables = tableData.flatMap{x => x.map(y => {
      TableInfo(( BaseTable(y._1), OtherTables(y._2(0), y._2(1), y._2(2))))})})
  }
}
