package com.maogogo.dolphin.services.impl

import com.maogogo.dolphin.services.Transform
import org.apache.spark.sql.DataFrame
import com.maogogo.dolphin.models._

class OrcTransformImpl {
  def from(model: TransformModel): DataFrame = {
    ???
  }

  def to(model: TransformModel, data: DataFrame): Unit = {
    ???
  }
}