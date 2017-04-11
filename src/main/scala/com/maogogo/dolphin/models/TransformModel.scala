package com.maogogo.dolphin.models

import scala.io.Source

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

case class DBSourceModel(name: String, url: String, username: String, password: String) {
  def getDriver: String = {
    url match {
      case x if x.startsWith("jdbc:mysql") => "com.mysql.jdbc.Driver"
      case x if x.startsWith("jdbc:oracle:thin") => "oracle.jdbc.driver.OracleDriver"
      case _ => throw new Exception("not supported")
    }
  }
}

case class ColumnModel(name: String, cname: Option[String], typeName: Option[String] = None, nullable: Boolean = true)

case class TransformModel(source: FromSource, dbSource: Option[Seq[DBSourceModel]], process: Option[Process],
  subModel: Option[TransformModel], target: Option[ToTarget])

