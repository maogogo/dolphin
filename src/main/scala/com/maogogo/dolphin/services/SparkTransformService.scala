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

  def transforms(models: Seq[TransformModel], rowData: Option[Map[String, Any]] = None): Unit = {

    def transform(model: TransformModel): Unit = {
      val _data = toDataFrame(sqlContext, rowData)(model.source)
      val data = model.process match {
        case Some(process) => toProcess(sqlContext, rowData)(model.source)
        case _ => _data
      }
      toTmpTable(data)(sqlContext)(model.source.getTempTableName(rowData))
      model.subModel match {
        case Some(smodel) =>
          val kv = data.cache.map { row => Map(data.columns.zip(row.toSeq): _*) }
          kv.map { row =>
            transforms(Seq(smodel), Some(row))
          }

        case _ => Unit
      }
      model.target match {
        case Some(target) => toTarget(data)(sqlContext, rowData)(target)
        case _ => Unit
      }
    }

    models.map { model =>
      //TODO hadoop source 
      model.source match {
        case source: HadoopSource =>
          val haddopConfig = new Configuration
          val hdfs = FileSystem.get(haddopConfig)
          toDoHadoop(haddopConfig, hdfs)(source)
        case _ => transform(model)
      }
    }
  }

  def toDoHadoop(haddopConfig: Configuration, hdfs: FileSystem): PartialFunction[FromSource, Unit] = {
    case remove: ActionRemovePath => hdfs.delete(new Path(remove.target), true)
    case move: ActionMovePath =>
      FileUtil.copy(
        hdfs, new Path(move.from),
        hdfs, new Path(move.to),
        move.deleteSource,
        haddopConfig)
    case merge: ActionMergeFile =>
      FileUtil.copyMerge(
        hdfs, new Path(merge.from),
        hdfs, new Path(merge.to),
        merge.deleteSource,
        haddopConfig,
        null)
    case getMerge: ActionGetMergeFile =>
      FileUtil.copyMerge(
        hdfs, new Path(getMerge.from),
        hdfs, new Path(getMerge.to),
        getMerge.deleteSource,
        haddopConfig,
        null)

      hdfs.copyToLocalFile(new Path(getMerge.to), new Path(getMerge.local))

  }

  def toDataFrame(implicit sqlContent: SQLContext, rowData: Option[Map[String, Any]] = None): PartialFunction[FromSource, DataFrame] = {
    case model: CSVSource =>
      sqlContent.read.format("com.databricks.spark.csv").schema(model.getSchema).options(Map("path" -> model.getPath((rowData)),
        "header" -> "false", "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).load
    case model: ParquetSource => sqlContext.read.parquet(model.getPath)
    case model: ORCSource => sqlContext.read.orc(model.getPath)
    case model: SQLSource => sqlContext.sql(model.getSQL)
  }

  def toTmpTable(data: DataFrame)(implicit sqlContent: SQLContext): PartialFunction[String, Unit] = {
    case tmpTable if !tmpTable.isEmpty =>
      data.registerTempTable(tmpTable)
  }

  def toProcess(implicit sqlContent: SQLContext, rowData: Option[Map[String, Any]] = None): PartialFunction[FromSource, DataFrame] = {
    case process: SQLProcess => sqlContext.sql(process.getSQL)
  }

  def toTarget(data: DataFrame)(implicit sqlContent: SQLContext, rowData: Option[Map[String, Any]] = None): PartialFunction[ToTarget, Unit] = {
    case target: CSVTarget =>
      println("====>>>>csv")
      //TODO 这里应该指定分区数
      data.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(Map("path" -> target.getPath(rowData), "header" -> "false",
        "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).save
    case target: ParquetTarget =>
      println("====>>>>parquet")
      data.write.mode(target.saveMode).parquet(target.getPath(rowData))
    case target: ORCTarget =>
      println("====>>>>orc")
      data.write.mode(target.saveMode).orc(target.getPath(rowData))
  }

}