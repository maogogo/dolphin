name := "dolphin"

version := "0.0.1"

organization := "com.maogogo"

scalaVersion := "2.10.6"

//resolvers += "sonatype-oss-snapshot" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-hive" % "1.6.1",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "mysql" % "mysql-connector-java" % "5.1.39",
  "org.apache.commons" % "commons-csv" % "1.4",
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.hbase" % "hbase" % "0.98.24-hadoop2",
  "org.apache.hbase" % "hbase-client" % "0.98.24-hadoop2",
  "org.apache.hbase" % "hbase-common" % "0.98.24-hadoop2",
  "org.apache.hbase" % "hbase-server" % "0.98.24-hadoop2"
  
  //"com.typesafe.play" %% "twirl-api" % "1.3.0"
  
  
)
