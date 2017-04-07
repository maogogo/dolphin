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
 * columns: 指定CSV 列
 * subTransform: 子任务
 *
 */
case class TransformModel(from: String, to: String, fromPath: Option[String], toPath: Option[String], format: Option[String],
    table: Option[String], sql: Option[String], tmpTable: Option[String], hiveTable: Option[String], mode: Option[String],
    process: Option[String], cache: Option[Boolean], db: Option[Seq[DBSourceModel]],
    columns: Option[Seq[ColumnModel]], subTransform: Option[TransformModel]) {

  val sqlP = "(?i)select.*".r.pattern
  val reg = """\{(.*?)\}""".r
  val subfix = "1 = ? AND 1 < ?"

  def sqling(kv: Option[Map[String, Any]] = None): String = {

    val tmpSQL = sql match {
      case Some(s) if !s.isEmpty && s.trim.toLowerCase.startsWith("select") => s.trim
      case Some(s) if !s.isEmpty =>
        Source.fromFile(s).getLines.mkString(" ").stripMargin.trim
      case _ => ""
    }

    replaceStr(tmpSQL, kv)

  }

  /**
   * TODO
   */
  def partitionSQL: String = {

    val dbSql = sqling(None)

    val formIndex = dbSql.toLowerCase().lastIndexOf("from")
    val whereIndex = dbSql.toLowerCase().lastIndexOf("where")

    formIndex > whereIndex match {
      case true => s"${dbSql} where ${subfix}"
      case false => s"${dbSql} and ${subfix}"
    }

  }

  def hiveSchema: Option[StructType] = {
    columns.map { cols =>
      StructType(cols.map { x =>
        StructField(x.name, StringType, x.nullable)
      })
    }
  }

  def saveMode: SaveMode = {
    //    val append = "((?i)append)".r
    //    val error = "(?i)error".r
    //    val ignore = "((?i)ignore)".r
    mode match {
      case Some(s) if s.toLowerCase == "append" => SaveMode.Append
      case Some(s) if s.toLowerCase == "error" => SaveMode.ErrorIfExists
      case Some(s) if s.toLowerCase == "ignore" => SaveMode.Ignore
      case _ => SaveMode.Overwrite
    }
  }

  def targetPath(kv: Option[Map[String, Any]] = None): String = {
    println("toPath.getOrElse() ===>>>" + toPath.getOrElse(""))
    replaceStr(toPath.getOrElse(""), kv, false)
  }

  private[this] def replaceStr(source: String, kv: Option[Map[String, Any]] = None, flag: Boolean = true): String = {

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
