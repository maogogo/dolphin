package com.maogogo.dolphin.modules

import com.maogogo.dolphin.models._
import scala.xml._
import com.maogogo.dolphin.models.CSVSource
import org.apache.derby.impl.store.raw.data.RemoveFile
import org.apache.spark.sql.SaveMode

trait ServicesModule { self =>

  def provideDolphinModel(file: String, params: Map[String, String]): Seq[TransformModel] = {

    val dolphin = XML.load(file)

    val sources = (dolphin \\ "dolphin" \\ "dbsources" \\ "source").map { source =>
      DBSourceModel(toAttributeText("name")(source).getOrElse(""), toAttributeText("url")(source).getOrElse(""),
        toAttributeText("username")(source).getOrElse(""), toAttributeText("password")(source).getOrElse(""))
    }

    (dolphin \\ "transforms" \\ "transform").map { node =>
      val childs = nodeToSubTransform(params, Some(sources))(node)
      nodeToTransform(Some(sources), childs, params)(node)
    }
  }

  private[this] def nodeToColumns: PartialFunction[Node, Option[Seq[ColumnModel]]] = {
    case node =>
      node.child.find(_.label == "columns").headOption.map {
        _.child.filter(_.label == "column").map { node =>
          val nullable = toAttributeText("nullable")(node).getOrElse("") match {
            case s if (s.toLowerCase() == "true") || s == "1" => true
            case _ => false
          }

          ColumnModel(toAttributeText("name")(node).getOrElse(""), toAttributeText("cname")(node),
            toAttributeText("type")(node), nullable)
        }
      }
  }

  private[this] def nodeToTransform(dbs: Option[Seq[DBSourceModel]] = None,
    childs: Option[TransformModel], params: Map[String, String]): PartialFunction[Node, TransformModel] = {
    case node =>
      val columns = nodeToColumns(node)

      val from = toAttributeText("from")(node).getOrElse("")
      val toOption = toAttributeText("to")(node)
      val fromPath = toAttributeText("fromPath")(node).getOrElse("")
      val toPath = toAttributeText("toPath")(node).getOrElse("")

      val modeOption = toAttributeText("mode")(node)
      val sql = toAttributeText("sql")(node)
      val tableOption = toAttributeText("table")(node)
      val tmpTable = toAttributeText("tmpTable")(node)
      val processOption = toAttributeText("process")(node)
      val format = toAttributeText("format")(node)
      val local = toAttributeText("local")(node)

      val partitionsOption = toAttributeText("partitions")(node)
      val conditionsOption = toAttributeText("conditions")(node)

      val source: FromSource = from match {
        case "csv" => CSVSource(fromPath, format, columns.getOrElse(Seq.empty), tmpTable)
        case "parquet" => ParquetSource(fromPath, sql, tmpTable)
        case "orc" => ORCSource(fromPath, sql, tmpTable)
        case "sql" => SQLSource(params, sql.getOrElse(""), tmpTable)
        case "hadoop" =>

          val actionOption = toAttributeText("action")(node)
          val deleteSourceOption = toAttributeText("deleteSource")(node)

          val deleteSource = deleteSourceOption match {
            case Some(s) if !s.isEmpty && s.toLowerCase() == "true" => true
            case _ => false
          }

          actionOption match {
            case Some("remove") => ActionRemovePath(fromPath)
            case Some("merge") => ActionMergeFile(fromPath, toPath, deleteSource)
            case Some("move") => ActionMovePath(fromPath, toPath, deleteSource)
            case Some("getmerge") => ActionGetMergeFile(fromPath, toPath, local.getOrElse(""), deleteSource)
            case _ => OtherAction(fromPath, toPath)
          }
        //HadoopSource(action)
        case x =>
          val jdbc = dbs.flatMap(_.find(_.name == x))

          if (jdbc.isDefined)
            DBSource(from, jdbc.get, tableOption.getOrElse(""), conditionsOption.getOrElse(""), partitionsOption.getOrElse("10").toInt, tmpTable)
          else
            OtherSource(fromPath)
      }

      val saveMode = modeOption match {
        case Some(s) if s.toLowerCase == "append" => SaveMode.Append
        case Some(s) if s.toLowerCase == "error" => SaveMode.ErrorIfExists
        case Some(s) if s.toLowerCase == "ignore" => SaveMode.Ignore
        case _ => SaveMode.Overwrite
      }

      val target: Option[ToTarget] = toOption match {
        case Some("csv") => Some(CSVTarget(toPath, tmpTable))
        case Some("parquet") => Some(ParquetTarget(toPath, saveMode, tmpTable))
        case Some("orc") => Some(ORCTarget(toPath, saveMode, tmpTable))
        case _ => None
      }

      val process: Option[Process] = processOption match {
        case Some(s) if !s.isEmpty => Some(SQLProcess(params, s))
        case _ => None
      }

      TransformModel(source, dbs, process, childs, target)

    //      TransformModel(toAttributeText("from")(node).getOrElse(""), toAttributeText("to")(node).getOrElse(""),
    //        toAttributeText("fromPath")(node), toAttributeText("toPath")(node), toAttributeText("format")(node),
    //        toAttributeText("table")(node), toAttributeText("sql")(node), toAttributeText("tmpTable")(node),
    //        toAttributeText("hiveTable")(node), toAttributeText("mode")(node), toAttributeText("process")(node),
    //        toAttributeText("cache")(node).map(_.toBoolean), dbs, columns, childs)
  }

  private[this] def nodeToSubTransform(params: Map[String, String], dbs: Option[Seq[DBSourceModel]] = None): PartialFunction[Node, Option[TransformModel]] = {
    case node =>
      node.child.find(_.label == "subtransform").headOption.map { nodeToTransform(dbs, None, params) }
  }

  private[this] def toAttributeText(name: String): PartialFunction[Node, Option[String]] = {
    case node => node.attribute(name).flatMap(_.headOption).map(_.text)
  }

}