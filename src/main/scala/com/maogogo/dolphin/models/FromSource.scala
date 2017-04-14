package com.maogogo.dolphin.models

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import scala.io.Source
import com.maogogo.dolphin.services.TemplateService

trait FromSource {

  val path: String
  val tmpTable: Option[String]
  //val params: Map[String, String]

  val reg = """\{(.*?)\}""".r

  def getPath(implicit rowData: Option[Map[String, Any]] = None): String = {
    path match {
      case p if !p.isEmpty => __replaceStr(p, rowData)
      case _ => ""
    }
  }

  def getTempTableName(rowData: Option[Map[String, Any]] = None): Option[String] = {
    tmpTable match {
      case Some(name) if !name.isEmpty => Some(__replaceStr(name, rowData))
      case _ => None
    }
  }

  def __getSQL(sql: String, params: Map[String, String], rowData: Option[Map[String, Any]] = None): String = {
    val tmpSQL = sql match {
      case s if !s.isEmpty && s.trim.toLowerCase.startsWith("select") => s.trim
      case s if !s.isEmpty =>
        Source.fromFile(s).getLines.mkString(" ").stripMargin.trim
      case _ => ""
    }
    __replaceStr(TemplateService.getContext(tmpSQL, params), rowData, true)
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

case class CSVSource(path: String, format: Option[String], columns: Seq[ColumnModel], val tmpTable: Option[String]) extends FromSource {

  def getSchema: StructType =
    StructType(columns.map { x =>
      StructField(x.name, StringType, x.nullable)
    })
}

case class ParquetSource(path: String, sql: Option[String], tmpTable: Option[String]) extends FromSource

case class ORCSource(path: String, sql: Option[String], tmpTable: Option[String]) extends FromSource

case class SQLSource(params: Map[String, String], sql: String, tmpTable: Option[String] = None) extends FromSource {
  val path: String = ""
  def getSQL(implicit rowData: Option[Map[String, Any]] = None): String = __getSQL(sql, params)
}

case class OtherSource(path: String, tmpTable: Option[String] = None) extends FromSource //(path: String, from: String, to: Option[String], sql: Option[String]) 

case class DBSource(path: String, dbs: DBSourceModel, table: String, conditions: String, partitions: Int, tmpTable: Option[String]) extends FromSource {

  //private[this] val conditions = "1 = ? AND 1 < ?"

  //  def partitionSQL: String = {
  //
  //    val formIndex = sql.toLowerCase().lastIndexOf("from")
  //    val whereIndex = sql.toLowerCase().lastIndexOf("where")
  //
  //    formIndex > whereIndex match {
  //      case true => s"${sql} where ${conditions}"
  //      case false => s"${sql} and ${conditions}"
  //    }
  //
  //  }
}