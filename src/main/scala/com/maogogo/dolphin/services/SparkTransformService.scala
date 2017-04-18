package com.maogogo.dolphin.services

import org.apache.spark._
import org.apache.spark.sql.hive._
import com.maogogo.dolphin.models._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.JdbcRDD
import java.sql._
import com.databricks.spark.csv._
import com.databricks.spark.csv.CsvRelation
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.util.Properties
import java.io.File

/**
 * parquet 或是 orc(没测试) 文件才能创建外部表
 *
 * 如果是 csv文件, 先 create table 再load data
 */
class SparkTransformService(implicit sc: SparkContext) extends Serializable {

  implicit val sqlContext = new SQLContext(sc)

  def transforms(models: Seq[TransformModel], rowData: Option[Map[String, Any]] = None): Unit = {

    def transform(model: TransformModel): Unit = {
      val _data = toDataFrame(sqlContext, rowData)(model.source)

      _data.printSchema
      toTmpTable(_data)(sqlContext)(model.source.getTempTableName(rowData))

      val data = model.process match {
        case Some(process) => toProcess(sqlContext, rowData)(process)
        case _ => _data
      }

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
        case source @ OtherSource(from, _) =>
          println(s"transform from [${from}] can not parse")
        case _ => transform(model)
      }
    }
  }

  def toDoHadoop(haddopConfig: Configuration, hdfs: FileSystem): PartialFunction[FromSource, Unit] = {
    case remove: ActionRemovePath =>
      println(s"command hadoop to remove ${remove}")
      hdfs.delete(new Path(remove.target), true)
    case move: ActionMovePath =>
      println(s"command hadoop to move ${move}")
      FileUtil.copy(
        hdfs, new Path(move.from),
        hdfs, new Path(move.to),
        move.deleteSource,
        haddopConfig)
    case merge: ActionMergeFile =>
      println(s"command hadoop to merge ${merge}")
      FileUtil.copyMerge(
        hdfs, new Path(merge.from),
        hdfs, new Path(merge.to),
        merge.deleteSource,
        haddopConfig,
        null)
    case getMerge: ActionGetMergeFile =>
      println(s"command hadoop to getmerge ${getMerge}")
      //val tmpPath = getMerge.to + File.separator + System.currentTimeMillis
      FileUtil.copyMerge(
        hdfs, new Path(getMerge.from),
        hdfs, new Path(getMerge.to),
        getMerge.deleteSource,
        haddopConfig,
        null)

      hdfs.copyToLocalFile(new Path(getMerge.to), new Path(getMerge.local))
      hdfs.delete(new Path(getMerge.to), true)
    case _ =>
      println("can not found hadoop command please use 'remove' 'move' 'merge' or 'getmerge'")

  }

  def toDataFrame(implicit sqlContent: SQLContext, rowData: Option[Map[String, Any]] = None): PartialFunction[FromSource, DataFrame] = {
    case model: CSVSource =>
      println(s"start source from csv ${model}")
      import org.apache.commons.csv.Lexer
      //      .option("treatEmptyValuesAsNulls", "true" )
      sqlContent.read.format("com.databricks.spark.csv").schema(model.getSchema).options(Map("path" -> model.getPath((rowData)),
        "header" -> "false", "delimiter" -> "|", "nullValue" -> "", //"ignoreTrailingWhiteSpace" -> "true", "ignoreTrailingWhiteSpace" -> "true",
        "charset" -> "UTF-8", "mode" -> "DROPMALFORMED", //"parserLib" -> "UNIVOCITY", 
        "quote" -> "\"", "escape" -> "\\",
        "inferSchema" -> "true", "treatEmptyValuesAsNulls" -> "true")).load //.repartition(500)
    case model: ParquetSource =>
      println(s"start source from parquet ${model}")
      sqlContext.read.parquet(model.getPath)
    case model: ORCSource =>
      println(s"start source from orc ${model}")
      sqlContext.read.orc(model.getPath)
    case model: SQLSource =>
      println(s"start source from sql ${model}")
      sqlContext.sql(model.getSQL)
    case model: DBSource =>
      println(s"start source from jdbc ${model}")
      val props = new Properties
      props.put("user", model.dbs.username)
      props.put("password", model.dbs.password)

      sqlContext.read.jdbc(model.dbs.url, model.table, props)

  }

  def toTmpTable(data: DataFrame)(implicit sqlContent: SQLContext): PartialFunction[Option[String], Unit] = {
    case Some(tmpTable) if !tmpTable.isEmpty =>
      data.registerTempTable(tmpTable)
    case _ => Unit
  }

  def toProcess(implicit sqlContent: SQLContext, rowData: Option[Map[String, Any]] = None): PartialFunction[Process, DataFrame] = {
    case process: SQLProcess => sqlContext.sql(process.getSQL)
  }

  def toTarget(data: DataFrame)(implicit sqlContent: SQLContext, rowData: Option[Map[String, Any]] = None): PartialFunction[ToTarget, Unit] = {
    case target: CSVTarget =>
      println(s"start target to csv ${target}")
      //TODO 这里应该指定分区数
      data.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(Map("path" -> target.getPath(rowData), "header" -> "false",
        "delimiter" -> "|", "nullValue" -> "", "dateFormat" -> "yyyyMMddHHmmss", "quote" -> "\"", "treatEmptyValuesAsNulls" -> "true")).save
      printExternalDDL(target, data)
      println(s"end target to csv")
    case target: ParquetTarget =>
      println(s"start target to parquet ${target}")
      data.repartition(200).write.mode(target.saveMode).parquet(target.getPath(rowData))
      printExternalDDL(target, data)
      println(s"end target to parquet")
    case target: ORCTarget =>
      println(s"start target to orc ${target}")
      data.write.mode(target.saveMode).orc(target.getPath(rowData))
      println(s"end target to orc")
  }

  def printExternalDDL(target: ToTarget, data: DataFrame): Unit = {
    target match {
      case target @ CSVTarget(path, Some(tmpTable)) =>
        //println(s"start target to csv ${target}")

        val schema = data.schema.fields.map { fields =>
          s"${fields.name} ${fields.dataType.typeName.toUpperCase}"
        }
        val external = s""" 
DROP TABLE IF EXISTS ${tmpTable};
CREATE EXTERNAL TABLE ${tmpTable} 
(${schema.mkString(",\n")}) 
ROW FORMAT DELIMITED FIELDS TERMINATED by '|' 
STORED AS TEXTFILE 
LOCATION '${path}';"""
        println(external.stripMargin)

      case target @ ParquetTarget(path, _, Some(tmpTable)) =>
        val schema = data.schema.fields.map { fields =>
          s"${fields.name} ${fields.dataType.typeName.toUpperCase}"
        }
        val external = s"""
DROP TABLE IF EXISTS ${tmpTable};
CREATE TABLE ${tmpTable} (
${schema.mkString(",\n")}
) USING org.apache.spark.sql.parquet 
OPTIONS (PATH '${path}');"""
        println(external.stripMargin)
      case _ =>
        println("nothing to do")
    }
  }

}