package com.maogogo.dolphin

import java.io.File

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SQLContext

import com.maogogo.dolphin.modules.ServicesModule
import com.maogogo.dolphin.services._
import com.typesafe.config.ConfigFactory

/**
 * 创建外部表
 * 1、hadoop txt(csv) to hive 外部表
 * 	toParquet to 外部表
 *  toOrc to 外部表
 * 2、mysql to hive 外部表
 * 3、json to hive 外部表
 * 4、hive 表 到 hive 外部表
 * 5、mongodb to hive 外部表
 *
 *
 *
 * 待完成
 * 1、外界参数
 * 2、orc数据问题
 * 3、
 *
 * sbt "run demo2.xml params.json"
 */
object Main extends ServicesModule {

  private[this] val Log = Logger.getLogger("dolphin")
  private[this] val conf: SparkConf = new SparkConf().setAppName("TestEmpno") //.setMaster("spark://60.205.127.163:7077")
  implicit val sc: SparkContext = new SparkContext(conf)
  implicit val sqlContent: SQLContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {

    PropertyConfigurator.configure("log4j.properties")

    println(logo)

    if (args == null || args.length < 1) {
      Log.error("main function has no arguments")
      throw new Exception("please input conf param file path")
    }

    implicit val config = ConfigFactory parseFile (new File(args(0))) resolve

    try {

      val path = TemplateService.createTemplate
      println(s"create template path: $path")
      val models = provideDolphinModel(path)

      val spark = new SparkTransformService
      println("models ==>>" + models)
      spark.transform(models)

      sc.stop

      println(s"${"=" * 10}SparkContext closed${"=" * 10}")
    } catch {
      case e: Throwable =>
        Log.error("exception", e)
    }

  }

  lazy val logo = """
      ____        __      __    _     
     / __ \____  / /___  / /_  (_)___ 
    / / / / __ \/ / __ \/ __ \/ / __ \
   / /_/ / /_/ / / /_/ / / / / / / / /
  /_____/\____/_/ .___/_/ /_/_/_/ /_/ 
               /_/                    """
}