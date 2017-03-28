package com.maogogo.dolphin.services

import org.apache.spark._
import org.apache.spark.sql.hive._
import com.maogogo.dolphin.models._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager
import java.sql.Connection

class HiveTransform(implicit sc: SparkContext) extends Transform {

  def from(models: Seq[TransformModel])(implicit hiveContext: HiveContext): Unit = {

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
      }
    }
  }

  def to(model: TransformModel, data: DataFrame)(implicit hiveContext: HiveContext): Unit = {
    ???
  }

  def fromSQL(model: TransformModel)(implicit hiveContext: HiveContext): DataFrame = {
    val data = hiveContext.sql(model.sql.getOrElse(""))
    data.printSchema
    data.registerTempTable(model.tmpTable.getOrElse(""))
    data
  }

  def fromCSV(model: TransformModel)(implicit hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    val schema = model.columns.map { x =>
      StructType(x.map { column =>
        StructField(column.cname.getOrElse(""), StringType, column.nullable)
      })
    }

    val data = hiveContext.read.schema(schema.get).format("com.databricks.spark.csv").options(Map("path" -> model.fromPath.getOrElse(""),
      "header" -> "false", "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).load

    data.printSchema
    hiveContext.createExternalTable(model.tmpTable.getOrElse(""), model.fromPath.getOrElse(""))
    data

  }

  def toCSV(model: TransformModel, data: DataFrame) = {
    data.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(Map("path" -> model.toPath.getOrElse(""), "header" -> "false",
      "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).save
  }

  def fromParquet(model: TransformModel)(implicit hiveContext: HiveContext): DataFrame = {
    val data = hiveContext.read.parquet(model.from)
    data.printSchema
    data.registerTempTable(model.tmpTable.getOrElse(""))
    data
  }

  def toParquet(model: TransformModel, data: DataFrame) = {
    //TODO 这里会表名有问题
    //注册临时表
    data.write.mode(SaveMode.Overwrite).parquet(model.toPath.getOrElse(""))
  }

  /**
   * from orc 有问题, 这里不能这么处理
   */
  def fromOrc(model: TransformModel)(implicit hiveContext: HiveContext): DataFrame = {
    val data = hiveContext.read.orc(model.fromPath.getOrElse(""))
    data.registerTempTable(model.tmpTable.getOrElse(""))
    data
  }

  def toOrc(model: TransformModel, data: DataFrame) = {
    data.write.mode(SaveMode.Overwrite).orc(model.toPath.getOrElse(""))
    //    val hiveContent = new HiveContext(sc)
    //    val hiveData = hiveContent.createDataFrame(data.rdd, data.schema)
    //    //TODO 这里应该考虑删除原来的表
    //    hiveContent.sql(s"drop table if exists ${model.tableName.getOrElse("")}")
    //    hiveData.printSchema
    //    hiveData.write.mode(SaveMode.Overwrite).orc(model.toPath.getOrElse(""))

    //    model.tableName match {
    //      case Some(name) if !name.isEmpty =>
    //        hiveContent.sql(s"drop table if exists $name")
    //        hiveContent.createExternalTable(name, model.toPath.getOrElse(""))
    //      //createExTable(data, name, model.toPath.getOrElse(""))(hiveContent)
    //      case _ =>
    //    }
  }

  def fromDB(model: TransformModel)(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    println("sql ==>>>" + model.dbSql)
    println("hivesql =>>" + model.hiveSQL)
    
    val jdbc = model.db.flatMap(_.find(_.name == model.from))

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
    
    data
  }

  def getStructType(db: DBSourceModel, sql: String): StructType = {
    val conn: Connection = DriverManager.getConnection(db.url, db.username, db.password)
    val pstmt = conn.prepareStatement(sql)
    val rs = pstmt.executeQuery
    val rsmd = rs.getMetaData
    val count = rsmd.getColumnCount

    StructType((1 to count).map(rsmd.getColumnName).map {
      case "JOIN" => StructField("JOIN2", StringType, true)
      case x => StructField(x, StringType, true)
    })
  }

}