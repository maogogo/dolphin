//package com.maogogo.dolphin
//
//import kafka.serializer._
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._
//
////import kafka.serializer.StringDecoder
//
//object Test {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("UserClickCountStat")
//    val ssc = new StreamingContext(conf, Seconds(5))
//    val topics = Set("toan")
//    val brokers = "localhost:9092"
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers, "zookeeper.connect" -> "localhost:20181", "serializer.class" -> "kafka.serializer.StringEncoder")
//
//    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//
//    val events = kafkaStream.map { line =>
//      println("line1 ===>>" + line._1)
//      println("line2 ===>>>" + line._2)
//      line._2
//    }
//    println("========================================")
//    events.print
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//}