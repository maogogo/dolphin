package com.maogogo.dolphin.services.impl

import com.maogogo.dolphin.services.Transform
import com.maogogo.dolphin.models._
import org.apache.spark.sql._

final class CsvTransform {
  
  def from(model: TransformModel): DataFrame = {
    ???
  }

  def to(model: TransformModel, data: DataFrame): Unit = {
    ???
  }
  
}