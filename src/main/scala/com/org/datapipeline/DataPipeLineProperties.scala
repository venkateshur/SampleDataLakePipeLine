package com.org.datapipeline

case class DataPipeLineProperties(tables: Option[List[TableInfo]] = None) extends Serializable

object DataPipeLineProperties{
  def apply(tableNames: Option[List[TableInfo]] = None): DataPipeLineProperties = new DataPipeLineProperties(tableNames)
}


