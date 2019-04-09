package com.org.datapipeline

case class KeyColumns(primaryKeyColumn: Array[String], sequenceIdColumn: String)
case class Tables(baseTable: String, backUpTable: String, historyTable: String)
case class TableInfo(tableInfo: (Tables, KeyColumns))
