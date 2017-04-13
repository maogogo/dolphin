package com.maogogo.dolphin.models

import org.apache.spark.sql.SaveMode

sealed trait ToTarget extends FromSource {
  val tmpTable: Option[String] = None
}

case class CSVTarget(params: Map[String, String], path: String) extends ToTarget

case class ParquetTarget(params: Map[String, String], path: String, saveMode: SaveMode) extends ToTarget

case class ORCTarget(params: Map[String, String], path: String, saveMode: SaveMode) extends ToTarget

case class OtherTarget(params: Map[String, String], path: String) extends ToTarget