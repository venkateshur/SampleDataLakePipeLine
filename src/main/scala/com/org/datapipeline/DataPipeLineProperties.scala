package com.org.datapipeline

case class DataPipeLineProperties(tables: List[TableInfo] = Nil, statusTable: String = " ") extends Serializable
