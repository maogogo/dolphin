package com.maogogo.dolphin.models

sealed trait Process

case class SQLProcess(sql: String) extends Process with FromSource {
  def getSQL: String = __getSQL(sql)
}