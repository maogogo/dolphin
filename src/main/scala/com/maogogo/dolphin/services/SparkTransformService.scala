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
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
 * parquet 或是 orc(没测试) 文件才能创建外部表
 *
 * 如果是 csv文件, 先 create table 再load data
 */
class SparkTransformService(implicit sc: SparkContext) {

  implicit val sqlContext = new SQLContext(sc)

  def transform(models: Seq[TransformModel]): Unit = {

    models.map { model =>
      //TODO hadoop source 
      val _data = toDataFrame(sqlContext)(model.source)
      val data = model.process match {
        case Some(process) => toProcess(sqlContext)(model.source)
        case _ => _data
      }
      model.subModel match {
        //TODO 这里需要一个kv Map
        case Some(smodel) => transform(Seq(smodel))
        case _ => Unit
      }
      model.target match {
        case Some(target) => toTarget(data)(sqlContext)(target)
        case _ => Unit
      }
    }
  }

  def toDataFrame(implicit sqlContent: SQLContext): PartialFunction[FromSource, DataFrame] = {
    case model: CSVSource =>
      sqlContent.read.format("com.databricks.spark.csv").schema(model.getSchema).options(Map("path" -> model.path,
        "header" -> "false", "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).load
    case model: ParquetSource => sqlContext.read.parquet(model.path)
    case model: ORCSource => sqlContext.read.orc(model.path)
    case model: SQLSource => sqlContext.sql(model.getSQL)
  }

  def toProcess(implicit sqlContent: SQLContext): PartialFunction[FromSource, DataFrame] = {
    case process: SQLProcess => sqlContext.sql(process.getSQL)
  }

  def toTarget(data: DataFrame)(implicit sqlContent: SQLContext): PartialFunction[ToTarget, Unit] = {
    case target: CSVTarget =>
      println("===csv>>>>>>>>>>>>>>" + target)
      //TODO 这里应该指定分区数
      data.write.format("com.databricks.spark.csv").options(Map("path" -> target.path, "header" -> "false",
        "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).save
    case target: ParquetTarget =>
      println("===parquet>>>>>>>>>>>>>>" + target)
      data.write.mode(target.saveMode).parquet(target.path)
    case target: ORCTarget =>
      println("===orc>>>>>>>>>>>>>>" + target)
      data.write.mode(target.saveMode).orc(target.path)
  }

}