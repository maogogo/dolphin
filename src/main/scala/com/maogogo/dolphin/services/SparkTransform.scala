package com.maogogo.dolphin.services

import com.maogogo.dolphin.models._
import org.apache.spark.sql._
import org.apache.spark.SparkContext

class SparkTransform(sc: SparkContext) extends Transform {
  
  implicit val sqlContent = new SQLContext(sc)
  

  def from(model: TransformModel)(implicit sqlContent: SQLContext): DataFrame = {
    ???
  }

  def to(model: TransformModel, data: DataFrame)(implicit sqlContent: SQLContext): Unit = {
    ???
  }
  
}