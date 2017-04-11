package com.maogogo.dolphin.models

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import scala.io.Source

trait FromSource {

  val reg = """\{(.*?)\}""".r
  
  def __getSQL(sql: String): String = {
    val tmpSQL = sql match {
      case s if !s.isEmpty && s.trim.toLowerCase.startsWith("select") => s.trim
      case s if !s.isEmpty =>
        Source.fromFile(s).getLines.mkString(" ").stripMargin.trim
      case _ => ""
    }
    __replaceStr(tmpSQL, None)
  }

  def __replaceStr(source: String, kv: Option[Map[String, Any]] = None, flag: Boolean = true): String = {

    kv.isDefined match {
      case true =>
        val ks = reg.findAllMatchIn(source).toList.map { _.group(1) }
        ks.fold(source) { (s, d) =>
          val _value = kv.flatMap(_.get(d).map(_.toString)).getOrElse("")
          s.replaceAll(s"\\{${d}\\}", if (flag) s"'${_value}'" else _value)
        }
      case false => source
    }

  }
}

case class CSVSource(path: String, format: Option[String], columns: Seq[ColumnModel], tmpTable: Option[String]) extends FromSource {
  def getSchema: StructType =
    StructType(columns.map { x =>
      StructField(x.name, StringType, x.nullable)
    })
}

case class ParquetSource(path: String, sql: Option[String], tmpTable: Option[String]) extends FromSource

case class ORCSource(path: String, sql: Option[String], tmpTable: Option[String]) extends FromSource

case class SQLSource(sql: String, tmpTable: Option[String]) extends FromSource {
  def getSQL: String = __getSQL(sql)
}

case class HadoopSource(action: Action) extends FromSource

case class OtherSource(path: String) extends FromSource//(path: String, from: String, to: Option[String], sql: Option[String]) 

case class DBSource(name: String, url: String, username: String, password: String) extends FromSource