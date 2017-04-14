package com.maogogo.dolphin.models

sealed trait Process

case class SQLProcess(params: Map[String, String], path: String) extends Process with FromSource {
  val tmpTable: Option[String] = None
  def getSQL(implicit rowData: Option[Map[String, Any]] = None): String = __getSQL(path, params)
}