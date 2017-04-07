package com.maogogo.dolphin.services

import org.apache.spark._
import org.apache.spark.sql.hive._
import com.maogogo.dolphin.models._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.JdbcRDD
import java.sql._
import com.databricks.spark.csv._
import scala.io.Source

/**
 * parquet 或是 orc(没测试) 文件才能创建外部表
 *
 * 如果是 csv文件, 先 create table 再load data
 */
class SparkTransform(implicit sc: SparkContext) extends Transform {

  //implicit val hiveContext = new HiveContext(sc)

  implicit val sqlContext = new SQLContext(sc)

  def transform(models: Seq[TransformModel]): Unit = {

    models.map { model =>
      val data = fromSource(model)

      val dataRDD = process(model, data)

      subTransform(model, dataRDD)

      toTarget(model, dataRDD)
    }
  }

  def subTransform(model: TransformModel, dataRDD: DataFrame)(implicit sqlContext: SQLContext): Unit = {

    if (model.subTransform.isDefined) {
      val tmpRDD = dataRDD.collect().map(r => Map(dataRDD.columns.zip(r.toSeq): _*))
      dataRDD.collect().map { r =>
        println("r ==>>>" + r)
      }

      tmpRDD.map { t =>
        println("t ==>>>" + t)

      }

      println("===================")
      tmpRDD.map { kv =>

        println("kv ===>>>>" + kv)
        //处理子流程
        model.subTransform.map { subModel =>
          val subData = fromSource(subModel, Some(kv))
          //TODO 暂时不支持 子 process
          //val subDataRDD = process(subModel, subData)
          toTarget(subModel, subData, Some(kv))
        }
      }
    }

  }

  def fromSource(model: TransformModel, kv: Option[Map[String, Any]] = None)(implicit sqlContext: SQLContext): DataFrame = {
    model.from match {
      case "csv" => fromCSV(model) //TODO 这里应该注册一个外部表
      case "parquet" => fromParquet(model)
      case "orc" => fromOrc(model)
      case "sql" => fromSQL(model, kv)
      case _ => fromDB(model)
    }
  }

  def toTarget(model: TransformModel, dataRDD: DataFrame, kv: Option[Map[String, Any]] = None)(implicit sqlContext: SQLContext): Unit = {
    model.to match {
      case "csv" => toCSV(model, dataRDD, kv)
      case "parquet" => toParquet(model, dataRDD)
      case "orc" => toOrc(model, dataRDD)
      case _ => Unit //throw new Exception("not supported to ")
    }
  }

  def process(model: TransformModel, data: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
    val sqlOrColumns = model.process match {
      case Some(s) if s.trim.contains(java.io.File.separator) =>
        Source.fromFile(s).getLines.mkString(" ").stripMargin.trim
      case Some(s) if !s.isEmpty => s.trim
      case _ => ""
    }

    sqlOrColumns match {
      case s if s.startsWith("select") => sqlContext.sql(s)
      case s if !s.isEmpty =>
        val cols = s.split(",").toSeq
        data.select(cols.head, cols.tail: _*)
      case _ => data
    }

  }

  def fromSQL(model: TransformModel, kv: Option[Map[String, Any]] = None)(implicit sqlContext: SQLContext): DataFrame = {
    println("kv =>>" + kv)
    println("sql ===>>>" + model.sqling(kv))
    val data = sqlContext.sql(model.sqling(kv))
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

    data

  }

  def toCSV(model: TransformModel, data: DataFrame, kv: Option[Map[String, Any]] = None) = {
    //data.saveAsCsvFile(model.toPath.getOrElse(""), Map("delimiter" -> "|", "header" -> "false"))
    println("model.targetPath(kv) ===>>>>" + model.targetPath(kv))
    data.write.format("com.databricks.spark.csv").mode(model.saveMode).options(Map("path" -> model.targetPath(kv), "header" -> "false",
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
    data.write.mode(model.saveMode).parquet(model.toPath.getOrElse(""))
  }

  /**
   * from orc 有问题, 这里不能这么处理
   * orc 必须使用HiveContext
   *
   */
  def fromOrc(model: TransformModel)(implicit sqlContext: SQLContext): DataFrame = {
    val data = sqlContext.read.orc(model.fromPath.getOrElse(""))
    //createTmpOrExTable(model, data)
    if (model.tmpTable.isDefined && !model.tmpTable.get.isEmpty)
      data.registerTempTable(model.tmpTable.get)
    data
  }

  def toOrc(model: TransformModel, data: DataFrame) = {
    data.write.mode(model.saveMode).orc(model.toPath.getOrElse(""))
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
      model.partitionSQL,
      1, 10, 1,
      { r =>
        val rsmd = r.getMetaData
        val count = rsmd.getColumnCount
        Row(Seq.range(1, count + 1).map(r.getString): _*)
      }).cache()

    //TODO 这里不应该是 None
    val schema = getStructType(jdbc.get, model.sqling(None))
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