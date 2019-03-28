package com.org.datapipeline.util
import com.org.datapipeline
import com.org.datapipeline.{BaseTable, DataPipeLineProperties, OtherTables, TableInfo}
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON


case class TableProperties(tableNames: List[(String, String)])

class Driver {

  var dataPipeLineProps: DataPipeLineProperties = DataPipeLineProperties()

  def loadTableProperties(propConfigPath: String)(sparkSession: SparkSession) {
    val jsonMap = JSON.parseFull(sparkSession.read.textFile(propConfigPath).collect().mkString(" ")).get.asInstanceOf[Map[String, Any]]
    val jsonProps = jsonMap("dataPipeLineProperties").asInstanceOf[Map[String,List[Map[String, List[String]]]]]
    createTableInfo(jsonProps("tableInfo"))

  }
  def createTableInfo(tableInfo: List[Map[String, List[String]]]): List[TableInfo] = {
    tableInfo.flatMap(x => x.map(y => datapipeline.TableInfo(BaseTable(y._1), OtherTables(y._2(0), y._2(1), y._2(2)))))
  }


}