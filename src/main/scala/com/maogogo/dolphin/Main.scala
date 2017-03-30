package com.maogogo.dolphin

import org.apache.spark._
import com.databricks.spark.csv._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import scala.xml._
import scala.collection.JavaConversions._
import com.maogogo.dolphin.modules.DolphinModule
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager
import java.sql.Connection
import com.maogogo.dolphin.modules.SparkModule
import com.maogogo.dolphin.models.TransformModel
import com.maogogo.dolphin.models.DBSourceModel
import java.util.Properties
import com.maogogo.dolphin.services.HiveTransform

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
 *
 */
object Main extends DolphinModule {

  private[this] val conf: SparkConf = new SparkConf().setAppName("TestEmpno") //.setMaster("app04")
  implicit val sc: SparkContext = new SparkContext(conf)
  implicit val sqlContent: SQLContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {

    println(logo)

//    val schema = StructType(Seq(
//      StructField("aa", StringType, true),
//      StructField("bb", StringType, true),
//      StructField("cc", StringType, true),
//      StructField("dd", StringType, true),
//      StructField("ee", StringType, true)))
//
//    val data = sqlContent.read.format("com.databricks.spark.csv").schema(schema).options(Map("path" -> "/data/in/toan/bb/aa.txt",
//      "header" -> "false", "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).load
//
//    data.collect().map { row =>
//      println("===>>" + row + "#######" + row.get(4))
//    }

    //    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "*********")
    //    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "*********")
    //    if (args == null || args.length != 1)
    //      throw new Exception("please set input xml file path")
    //
    //    println(s"load xml file ${args(0)}")

    //    val models = provideDolphinModel(args(0))
    //
    //    val hive = new HiveTransform
    //
    //    hive.transform(models)
    //
    //    //    val props = new Properties
    //    //    props.put("driver", "com.mysql.jdbc.Driver")
    //    //    props.put("user", "root")
    //    //    props.put("password", "@WSX4rfv")
    //    //    val hive = new HiveContext(sc)
    //    //    val data = hive.read.jdbc("jdbc:mysql://60.205.141.127:19306/taibao?useSSL=false", "t_component", props)
    //    //
    //    //    data.write.mode(SaveMode.Overwrite).saveAsTable("t_component") //.orc("/user/hive/warehouse/t_component/")
    //
    //    //    val data = hive.read.orc("/data/in/toan/t_channel_user/")
    //    //    data.registerTempTable("t_channel_orc_d")
    //    //
    //    //    //println("==================>>>>")
    //    //    hive.sql("select count(1) from t_channel_user").collect.map { row =>
    //    //      println("===>>>>>>" + row.get(0).toString)
    //    //    }
    //    //
    sc.stop
    //val model = provideDolphinModel("/cpic/bigdata/bigpm/Test/demo.xml")

    //    model.trans.map { tran =>
    //      //from => to
    //      val data = tran.from match {
    //        case "csv" =>
    //          fromCSV(tran)
    //        case "parquet" => fromParquet(tran)
    //        case "orc" => fromOrc(tran)
    //        case x =>
    //          val source = model.sources.find(_.name == x)
    //          if (source.isEmpty) throw new Exception(s"can not found source by name [$x]")
    //          fromDB(tran, source.get)
    //      }
    //
    //      tran.to match {
    //        case "csv" => toCSV(tran, data)
    //        case "parquet" => toParquet(tran, data)
    //        case "orc" => toOrc(tran, data)
    //        case _ => throw new Exception("not supported")
    //      }
    //    }
  }

  //  def fromCSV(model: TransformModel)(implicit sqlContent: SQLContext): DataFrame = {
  //    sqlContent.read.format("com.databricks.spark.csv").options(Map("path" -> model.fromPath.getOrElse(""), "header" -> "false",
  //      "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).load
  //  }
  //
  //  def toCSV(model: TransformModel, data: DataFrame)(implicit sqlContent: SQLContext) = {
  //    data.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(Map("path" -> model.hdfsToPath, "header" -> "false",
  //      "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).save
  //
  //    model.tableName match {
  //      case Some(name) if !name.isEmpty =>
  //        createExTable(data, name, model.toPath.getOrElse(""))
  //      case _ =>
  //    }
  //  }
  //
  //  def fromParquet(model: TransformModel)(implicit sqlContent: SQLContext): DataFrame = {
  //    sqlContent.read.parquet(model.from)
  //  }
  //
  //  def toParquet(model: TransformModel, data: DataFrame)(implicit sqlContent: SQLContext) = {
  //    //TODO 这里会表名有问题
  //    //注册临时表
  //    data.registerTempTable(model.tableName.getOrElse(""))
  //    data.printSchema
  //    data.write.parquet(model.toPath.getOrElse(""))
  //
  //    if (model.debug) {
  //      createExTable(data, model.tableName.getOrElse(""), model.toPath.getOrElse(""))
  //    }
  //
  //  }
  //
  //  /**
  //   * from orc 有问题, 这里不能这么处理
  //   */
  //  def fromOrc(model: TransformModel)(implicit sqlContent: SQLContext): DataFrame = {
  //    sqlContent.read.orc(model.fromPath.getOrElse(""))
  //  }
  //
  //  def toOrc(model: TransformModel, data: DataFrame)(implicit sc: SparkContext) = {
  //    val hiveContent = new HiveContext(sc)
  //    val hiveData = hiveContent.createDataFrame(data.rdd, data.schema)
  //    //TODO 这里应该考虑删除原来的表
  //    hiveContent.sql(s"drop table if exists ${model.tableName.getOrElse("")}")
  //    hiveData.printSchema
  //    hiveData.write.mode(SaveMode.Overwrite).orc(model.toPath.getOrElse(""))
  //
  //    //    model.tableName match {
  //    //      case Some(name) if !name.isEmpty =>
  //    //        hiveContent.sql(s"drop table if exists $name")
  //    //        hiveContent.createExternalTable(name, model.toPath.getOrElse(""))
  //    //      //createExTable(data, name, model.toPath.getOrElse(""))(hiveContent)
  //    //      case _ =>
  //    //    }
  //  }
  //
  //  def fromDB(model: TransformModel, db: DBSourceModel)(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {
  //
  //    println("hivesql =>>" + model.hiveSQL)
  //    println("sql ==>>>" + model.sql)
  //    val rdd = new JdbcRDD(
  //      sc,
  //      () => {
  //        Class.forName(db.getDriver)
  //        DriverManager.getConnection(db.url, db.username, db.password)
  //      },
  //      //TODO
  //      model.hiveSQL,
  //      1, 10, 1,
  //      { r =>
  //        val rsmd = r.getMetaData
  //        val count = rsmd.getColumnCount
  //        Row(Seq.range(1, count + 1).map(r.getString): _*)
  //      }).cache()
  //
  //    val schema = getStructType(db, model.sql)
  //    sqlContext.createDataFrame(rdd, schema)
  //
  //  }
  //
  //  def getStructType(db: DBSourceModel, sql: String): StructType = {
  //    val conn: Connection = DriverManager.getConnection(db.url, db.username, db.password)
  //    val pstmt = conn.prepareStatement(sql)
  //    val rs = pstmt.executeQuery
  //    val rsmd = rs.getMetaData
  //    val count = rsmd.getColumnCount
  //
  //    StructType((1 to count).map(rsmd.getColumnName).map {
  //      case "JOIN" => StructField("JOIN2", StringType, true)
  //      case x => StructField(x, StringType, true)
  //    })
  //  }
  //
  //  def createTmpTable(data: DataFrame, tableName: String) = {
  //    data.registerTempTable(tableName)
  //  }
  //
  //  def createExTable(data: DataFrame, tableName: String, path: String)(implicit sc: SparkContext) = {
  //    val hive = new HiveContext(sc)
  //    hive.createExternalTable(tableName, path)
  //  }

  //val model = DolphinModule.provideDolphinModel

  //csv file -> parquet (tmp table) -> sql -> parquet
  //csv file -> parquet (tmp table) -> sql -> csv
  // hdfs /data/in/abcd/abcd.txt

  //  println(logo)
  //  //  //
  //  val conf = new SparkConf().setAppName("TestEmpno") //.setMaster("app04")
  //  val sc = new SparkContext(conf)
  //  val sqlContext = new SQLContext(sc)
  //  //  //
  //  val props = new java.util.Properties
  //  props.put("driver", "com.mysql.jdbc.Driver")
  //  props.put("user", "root")
  //  props.put("password", "@WSX4rfv")
  //  //
  //  //  val allPath = "/data/in/bensong/all"
  //  //  val incPath = "/data/in/bensong/inc"
  //  //  val tmpPath = "/data/in/bensong/tmp"
  //  //
  //  //  props.put("driver", "oracle.jdbc.driver.OracleDriver")
  //  //  props.put("user", "channel")
  //  //  props.put("password", "channel")
  //  //props.put("dbtable", "channel")
  //  //val tabelName = "tbl_division"
  //  //  "dbtable" -> "***",
  //
  //  val url = "jdbc:mysql://60.205.141.127:19306/taibao?useSSL=false"
  //  //val url = "jdbc:oracle:thin:@10.182.52.9:1521/chnldb" //"jdbc:mysql://60.205.141.127:19306/taibao?useSSL=false"
  //  //"jdbc:oracle:thin:@10.182.52.10:1521/chnldb_DEV"
  //
  //  //    "tbl_division",
  //  //    "tbl_division_relation",
  //  //    "tbl_department",
  //  //    "tbl_department_relation",
  //  //    "tbl_master_group_relation",
  //  //    "tbl_career",
  //  //    "tbl_section_role_relation",
  //  //    "tbl_section",
  //  //    "s_version",
  //  //    "s_version_relation",
  //  //    "tbl_person",
  //  //    "tbl_person_role")
  //
  //  //  Class.forName("oracle.jdbc.driver.OracleDriver")
  //  //  val conn = DriverManager.getConnection(url, "channel", "channel")
  //  //val d = sqlContext.read.format("com.databricks.spark.csv").options(Map("path" -> s"/", "header" -> "false")).load
  //  //      "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).save
  //
  //  //val names = Seq("basic_law_id", "title", "value", "status", "cell_index", "t_date", "t_time")
  //
  //  //import sqlContext.implicits._
  //  val rdd = new JdbcRDD(
  //    sc,
  //    () => {
  //      Class.forName("com.mysql.jdbc.Driver")
  //      DriverManager.getConnection(url, "root", "@WSX4rfv")
  //    },
  //    "select * from t_basic_law_test where 1 = ? AND 1 < ?",
  //    1, 2, 1,
  //    { r =>
  //
  //      val rsmd = r.getMetaData
  //      //println("===>>>>>>>>" + rsmd)
  //      val count = rsmd.getColumnCount
  //      //println("count ===>>>" + count)
  //      Row(Seq.range(1, count).map(i => r.getString(i)): _*)
  //    }).cache()
  //
  //  val schema = StructType(Seq(StructField("basic_law_id", StringType, true), StructField("title", StringType, true),
  //    StructField("value", StringType, true), StructField("status", StringType, true),
  //    StructField("cell_index", StringType, true), StructField("t_date", StringType, true), StructField("t_time", StringType, true)))
  //
  //  val data = sqlContext.createDataFrame(rdd, schema)
  //  data.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").options(Map("path" -> s"/data/in/toan/tmp/t_basic_law_test/", "header" -> "false",
  //    "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).save
  //  //rdd.saveAsTextFile(path)
  //
  //  //  val names = tables.map { table =>
  //  //    getTableColumns(conn, table)
  //  }
  //
  //  def getTableColumns(conn: Connection, sql: String): Seq[String] = {
  //
  //    //Future {
  //    println(s"sql : \n\t $sql")
  //    val pstmt = conn.prepareStatement("select * from " + sql)
  //    val rs = pstmt.executeQuery
  //    val rsmd = rs.getMetaData
  //
  //    val count = rsmd.getColumnCount
  //
  //    (1 to count).map(rsmd.getColumnName).map {
  //      case "JOIN" => "JOIN2"
  //      case x => x
  //    }
  //    //}
  //  }

  //  tables.map { tn =>
  //
  //    println("===>>>>>>>>>>" + tn)
  //    val data = sqlContext.read.jdbc(url, tn, props)
  //    data.write.format("com.databricks.spark.csv").options(Map("path" -> s"${tmpPath}/${tn}/", "header" -> "false",
  //      "delimiter" -> "|", "nullValue" -> "", "treatEmptyValuesAsNulls" -> "true")).save
  //
  //    //    .option("delimiter", "|")
  //    //      .option("nullValue", "")
  //    //      .option("treatEmptyValuesAsNulls", "true")
  //
  //    //    val hc = new HiveContext(sc)
  //    //    hc.sql(s"drop table if exists ${tn._1}_all_ex")
  //    //
  //    //    val hiveData = hc.createDataFrame(data.rdd, jdbcData.schema)
  //    //    hiveData.printSchema
  //    //    hiveData.write.mode(SaveMode.Overwrite).orc(s"$allPath/orc/$name")
  //    //    hc.createExternalTable(s"${name}_all_ex", s"$allPath/orc/$name", "orc")
  //
  //  }

  //sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

  //  Class.forName("oracle.jdbc.driver.OracleDriver")
  //  //val d = new JdbcRDD
  //  import sqlContext.implicits._
  //  val data = new JdbcRDD(sc,
  //    () => DriverManager.getConnection(url, "channel", "channel"), tables(0), 10l, 100l, 1, { rs =>
  //      val rsmd = rs.getMetaData
  //      val count = rsmd.getColumnCount
  //      //while(rs.next()) {
  //      Seq.range(1, count + 1) //.map(rs.getString).tu
  //      //}
  //
  //    }) //.toDF("BUSLINE_NAME", "BUSLINE_NO", "CARTYPE", "STATION_NAME", "STATION_ID", "LATITUDE", "LONGTITUDE")

  //  val data = sqlContext.read.jdbc(url, "tbl_channelrole", props)
  //  data.registerTempTable("tmp_tbl_channelrole")
  //  val tpmData = sqlContext.sql(tables(0))
  //
  //  aa(tpmData, "tbl_channelrole", sc)
  //
  //  println("tbl_channlerole create success")

  //aa(tmpData, table, sc)
  //println(s"create $table success")

  //  val d = new Date()
  //  val date4mat = new SimpleDateFormat("yyyyMMdd")
  //  val date = date4mat.format(d)
  //  val (sc, sqlContext) = Initial_Spark.Initial_Spark("ZDRY_JF_DATAEX", date)

  //val data = sqlContext.read.jdbc(url, tabelName, props)

  //  val dateP = "(?i)date.*".r.pattern
  //  val dateTimeP = "(?i)datetime.*".r.pattern
  //  val timeP = "(?i)timestamp.*".r.pattern

  //data.write.mode(SaveMode.Overwrite).orc(s"$allPath/orc/$tabelName")

  //  val schema = StructType(data.schema.map {
  //    case x if dateP.matcher(x.dataType.typeName).matches ||
  //      dateTimeP.matcher(x.dataType.typeName).matches ||
  //      timeP.matcher(x.dataType.typeName).matches =>
  //
  //      println("===>>>>" + x.dataType)
  //
  //      x.copy(dataType = TimestampType)
  //    case x =>
  //      println("--->>>>>>>>>>" + x.dataType)
  //      x
  //  })
  //
  //  val tmpData = sqlContext.createDataFrame(data.rdd, schema)
  //
  //  tmpData.write.mode(SaveMode.Overwrite).parquet(s"$allPath/parquet/$tabelName")

  //  def aa(jdbcData: DataFrame, name: String, sc: SparkContext): Unit = {
  //
  //    val hc = new HiveContext(sc)
  //    hc.sql(s"drop table if exists ${name}_all_ex")
  //
  //    val hiveData = hc.createDataFrame(jdbcData.rdd, jdbcData.schema)
  //    hiveData.printSchema
  //    hiveData.write.mode(SaveMode.Overwrite).orc(s"$allPath/orc/$name")
  //    hc.createExternalTable(s"${name}_all_ex", s"$allPath/orc/$name", "orc")
  //  }

  //
  //  data.registerTempTable("t_basic_law_parquet")
  //  data.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/parquet/t_basic_law")

  //spark 1.6.1 这句会报错 提示使用HiveContext
  //sqlContext.createExternalTable("t_basic_law_ex", "/user/hive/warehouse/parquet/t_basic_law", "parquet")

  //  val hiveContext = new HiveContext(sc)
  //  hiveContext.createExternalTable("t_basic_law_ex", "/user/hive/warehouse/parquet/t_basic_law", "parquet")

  //sqlContext.sql(s"""create external table t_basic_law_ex like t_basic_law_parquet""")

  //  val parquetFile = sqlContext.read.parquet("/user/hive/warehouse/parquet/t_aabbcc2/")
  //
  //  parquetFile.registerTempTable("t_aabbccdd")
  //
  //  sqlContext.sql("select count(1) from t_aabbccdd").collect().map { row =>
  //    println("===>>>>>>>" + row.get(0))
  //  }

  //  //
  //  //  val parquetFile = sqlContext.read.parquet("/user/hive/warehouse/parquet/t_page/")
  //  //
  //  //  parquetFile.registerTempTable("t_ppt")
  //  //
  //  //  val teenagers = sqlContext.sql("SELECT * FROM t_ppt")
  //  //  teenagers.map(t => "Name: " + t(0)).collect().foreach(x => println("===>>>>>" + x))
  //
  //  def convert(sqlContext: SQLContext, filename: String, schema: StructType, tablename: String) {
  //    // import text-based table first into a data frame.
  //    // make sure to use com.databricks:spark-csv version 1.3+ 
  //    // which has consistent treatment of empty strings as nulls.
  //    val df = sqlContext.read
  //      .format("com.databricks.spark.csv")
  //      .schema(schema)
  //      .option("delimiter", "|")
  //      .option("nullValue", "")
  //      .option("treatEmptyValuesAsNulls", "true")
  //      .load(filename)
  //
  //    df.printSchema
  //
  //    // now simply write to a parquet file
  //    df.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/parquet/" + tablename)
  //
  //    df.registerTempTable(tablename)
  //  }
  //  //
  //  val schema = StructType(Array(
  //    StructField("id", StringType, true),
  //    StructField("name", StringType, true),
  //    StructField("age", StringType, true),
  //    StructField("ccdd", StringType, true)))
  //
  //  convert(sqlContext,
  //    "/data/in/abcd/abcd.txt",
  //    schema,
  //    "t_aabbcc")
  //
  //  sqlContext.sql("select name from t_aabbcc").write.parquet("/user/hive/warehouse/parquet/t_aabbcc2")

  //sc.stop()

  lazy val logo = """
      ____        __      __    _     
     / __ \____  / /___  / /_  (_)___ 
    / / / / __ \/ / __ \/ __ \/ / __ \
   / /_/ / /_/ / / /_/ / / / / / / / /
  /_____/\____/_/ .___/_/ /_/_/_/ /_/ 
               /_/                    """
}