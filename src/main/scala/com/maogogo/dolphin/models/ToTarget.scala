package com.maogogo.dolphin.models

import org.apache.spark.sql.SaveMode

sealed trait ToTarget

case class CSVTarget(path: String) extends ToTarget

case class ParquetTarget(path: String, saveMode: SaveMode) extends ToTarget

case class ORCTarget(path: String, saveMode: SaveMode) extends ToTarget

case class OtherTarget(path: String) extends ToTarget