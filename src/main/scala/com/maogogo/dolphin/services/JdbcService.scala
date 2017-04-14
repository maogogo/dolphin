package com.maogogo.dolphin.services

import com.maogogo.dolphin.models.DBSource
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.rdd.JdbcRDD
import java.sql._
import com.maogogo.dolphin.models.DBSourceModel
import org.apache.spark.sql.types._

trait JdbcService {

  def loadDataFrame(source: DBSource)(implicit sqlContext: SQLContext): DataFrame = {

    null
//
//    val rdd = new JdbcRDD(
//      sc,
//      () => {
//        Class.forName(jdbc.get.getDriver)
//        DriverManager.getConnection(jdbc.get.url, jdbc.get.username, jdbc.get.password)
//      },
//      //TODO
//      s"${source.sql} ${source.conditions}",
//      1, 1000, source.partitions,
//      { r =>
//        val rsmd = r.getMetaData
//        val count = rsmd.getColumnCount
//        Row(Seq.range(1, count + 1).map(r.getString): _*)
//      }).cache()
//
//    //TODO 这里不应该是 None
//    val schema = getStructType(jdbc.get, source.sql)
//    val data = sqlContext.createDataFrame(rdd, schema)
//
//    data.printSchema
//
//    data
  }

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