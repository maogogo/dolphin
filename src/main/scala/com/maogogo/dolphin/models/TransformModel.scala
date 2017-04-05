package com.maogogo.dolphin.models

import scala.io.Source
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
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

//from="mysql" to="parquet" fromPath="" toPath="" format="" table="" sql="" tmpTable="" hiveTable=""
/**
 *
 * from : 输入类型
 * to: 输出类型
 * fromPath: 输入地址
 * toPath: 输出地址
 * format: 格式化 CVS 有效
 * table: db 有效 查询表明
 * sql: 使用查询的sql或sql文件
 * hiveTable: 保存为hive的表
 *
 */
case class TransformModel(from: String, to: String, fromPath: Option[String], toPath: Option[String], format: Option[String],
    table: Option[String], sql: Option[String], tmpTable: Option[String], hiveTable: Option[String],
    db: Option[Seq[DBSourceModel]], columns: Option[Seq[ColumnModel]], childTransform: Option[TransformModel]) {
  val sqlP = "(?i)select.*".r.pattern
  val subfix = "1 = ? AND 1 < ?"

  def dbSql: String = {
    sql match {
      case Some(s) if !s.isEmpty && sqlP.matcher(s.trim).matches => s.trim
      case Some(s) if !s.isEmpty =>
        Source.fromFile(s).getLines.mkString(" ").stripMargin.trim
    }
  }

  /**
   * TODO
   * 如果中间有where的情况这样不支持
   */
  def partitionSQL: String = {
    dbSql match {
      case s if s.toLowerCase().contains("where") =>
        s"${s} and ${subfix}"
      case _ => s"${sql.getOrElse("")} where ${subfix}"
    }
  }

  def hiveSchema: Option[StructType] = {
    columns.map { cols =>
      StructType(cols.map { x =>
        StructField(x.name, StringType, x.nullable)
      })
    }
  }

}
