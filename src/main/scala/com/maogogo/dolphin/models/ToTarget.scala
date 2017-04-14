package com.maogogo.dolphin.models

import org.apache.spark.sql.SaveMode

sealed trait ToTarget extends FromSource

case class CSVTarget(path: String, tmpTable: Option[String] = None) extends ToTarget

case class ParquetTarget(path: String, saveMode: SaveMode, tmpTable: Option[String] = None) extends ToTarget

case class ORCTarget(path: String, saveMode: SaveMode, tmpTable: Option[String] = None) extends ToTarget

case class OtherTarget(path: String, tmpTable: Option[String] = None) extends ToTarget