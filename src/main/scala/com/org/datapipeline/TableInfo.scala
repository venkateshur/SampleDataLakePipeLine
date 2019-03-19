package com.org.datapipeline

case class BaseTable(baseTable: String)
case class OtherTables(backUpTable: String, historyTable: String, statsTable: String)
case class TableInfo (tableInfo: (BaseTable, OtherTables))

