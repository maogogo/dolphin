package com.maogogo.dolphin.services

import org.apache.spark._
import org.apache.spark.sql.hive._
import com.maogogo.dolphin.models._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.JdbcRDD
import java.sql._
import com.databricks.spark.csv._

/**
 * parquet 或是 orc(没测试) 文件才能创建外部表
 *
 * 如果是 csv文件, 先 create table 再load data
 */
class HiveTransform(implicit sc: SparkContext) extends Transform {

  //implicit val hiveContext = new HiveContext(sc)

  implicit val sqlContext = new SQLContext(sc)

  def transform(models: Seq[TransformModel]): Unit = {

    models.map { model =>
      val data = model.from match {
        case "csv" => fromCSV(model) //这里应该注册一个外部表
        case "parquet" => fromParquet(model)
        case "orc" => fromOrc(model)
        case "sql" => fromSQL(model)
        case x => fromDB(model)
      }

      model.to match {
        case "csv" => toCSV(model, data)
        case "parquet" => toParquet(model, data)
        case "orc" => toOrc(model, data)
        case _ => Unit //throw new Exception("not supported to ")
      }
    }
  }

  //  def fromSQL(model: TransformModel)(implicit hiveContext: HiveContext): DataFrame = {
  //    val data = hiveContext.sql(model.sql.getOrElse(""))
  //    data.printSchema
  //    createTmpOrExTable(model, data)
  //    data
  //  }

  def fromSQL(model: TransformModel)(implicit sqlContext: SQLContext): DataFrame = {
    val data = sqlContext.sql(model.sql.getOrElse(""))
    data.printSchema
    if (model.tmpTable.isDefined && !model.tmpTable.get.isEmpty) {
      //println("p_t_component_d ===>>>" + model.tmpTable.get)
      data.registerTempTable(model.tmpTable.get)
    }
    //createTmpOrExTable(model, data)
    data
  }

  def fromCSV(model: TransformModel)(implicit sqlContent: SQLContext): DataFrame = {
    //import hiveContext.implicits._
    //val sqlContent = new SQLContext(sc)
    //这里应该报错
    val schema = model.hiveSchema.get

    //    val data = sqlContent.csvFile(model.fromPath.getOrElse(""), false, '|')
    //    data.printSchema

    val data = sqlContent.read.format("com.databricks.spark.csv").schema(schema).options(Map("path" -> model.fromPath.getOrElse(""),
      "header" -> "false", "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).load
    //
    data.printSchema
    if (model.tmpTable.isDefined && !model.tmpTable.get.isEmpty) {
      //println("p_t_component_d ===>>>" + model.tmpTable.get)
      data.registerTempTable(model.tmpTable.get)
    }

    //    if (model.hiveTable.isDefined && !model.hiveTable.get.isEmpty) {
    //      val hiveContext = new HiveContext(sc)
    //      //hiveContext.createExternalTable(model.hiveTable.get, model.fromPath.getOrElse(""))
    //    }

    data

  }

  def toCSV(model: TransformModel, data: DataFrame) = {
    //data.saveAsCsvFile(model.toPath.getOrElse(""), Map("delimiter" -> "|", "header" -> "false"))
    data.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(Map("path" -> model.toPath.getOrElse(""), "header" -> "false",
      "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).save
  }

  def fromParquet(model: TransformModel)(implicit sqlContext: SQLContext): DataFrame = {
    val data = sqlContext.read.parquet(model.fromPath.getOrElse(""))
    data.printSchema
    if (model.tmpTable.isDefined && !model.tmpTable.get.isEmpty)
      data.registerTempTable(model.tmpTable.get)
    //createTmpOrExTable(model, data)
    data
  }

  def toParquet(model: TransformModel, data: DataFrame) = {
    data.write.mode(SaveMode.Overwrite).parquet(model.toPath.getOrElse(""))
  }

  /**
   * from orc 有问题, 这里不能这么处理
   */
  def fromOrc(model: TransformModel)(implicit sqlContext: SQLContext): DataFrame = {
    val data = sqlContext.read.orc(model.fromPath.getOrElse(""))
    //createTmpOrExTable(model, data)
    if (model.tmpTable.isDefined && !model.tmpTable.get.isEmpty)
      data.registerTempTable(model.tmpTable.get)
    data
  }

  def toOrc(model: TransformModel, data: DataFrame) = {
    data.write.mode(SaveMode.Overwrite).orc(model.toPath.getOrElse(""))
  }

  def fromDB(model: TransformModel)(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    val jdbc = model.db.flatMap(_.find(_.name == model.from))

    if (jdbc.isEmpty)
      throw new Exception(s"not supported from type ${model.from}")

    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName(jdbc.get.getDriver)
        DriverManager.getConnection(jdbc.get.url, jdbc.get.username, jdbc.get.password)
      },
      //TODO
      model.hiveSQL,
      1, 10, 1,
      { r =>
        val rsmd = r.getMetaData
        val count = rsmd.getColumnCount
        Row(Seq.range(1, count + 1).map(r.getString): _*)
      }).cache()

    val schema = getStructType(jdbc.get, model.dbSql)
    val data = sqlContext.createDataFrame(rdd, schema)

    data.printSchema
    data.registerTempTable(model.tmpTable.getOrElse(""))
    //    if (model.hiveTable.isDefined) {
    //      sqlContext.createExternalTable(model.hiveTable.getOrElse(""), model.fromPath.getOrElse(""))
    //    }

    data
  }

  //  def createTmpOrExTable(model: TransformModel, data: DataFrame)(implicit hiveContext: HiveContext) = {
  //    if (model.tmpTable.isDefined && !model.tmpTable.get.isEmpty)
  //      data.registerTempTable(model.tmpTable.get)
  //    if (model.hiveTable.isDefined && !model.hiveTable.get.isEmpty)
  //      hiveContext.createExternalTable(model.hiveTable.get, model.fromPath.getOrElse(""))
  //
  //  }

  /**
   * 这里的数据库连接没有关闭
   */
  def getStructType(db: DBSourceModel, sql: String): StructType = {
    val conn: Connection = DriverManager.getConnection(db.url, db.username, db.password)
    val pstmt = conn.prepareStatement(sql)
    val rs = pstmt.executeQuery
    val rsmd = rs.getMetaData
    val count = rsmd.getColumnCount

    val _type = StructType((1 to count).map(rsmd.getColumnName).map {
      case "JOIN" => StructField("JOIN2", StringType, true)
      case x => StructField(x, StringType, true)
    })

    pstmt.close
    conn.close
    _type
  }

}