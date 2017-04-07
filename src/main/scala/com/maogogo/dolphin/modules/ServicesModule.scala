package com.maogogo.dolphin.modules

import com.maogogo.dolphin.models._
import scala.xml._

trait ServicesModule { self =>

  def provideDolphinModel(file: String): Seq[TransformModel] = {

    val dolphin = XML.load(file)

    val sources = (dolphin \\ "dolphin" \\ "dbsources" \\ "source").map { source =>
      DBSourceModel(toAttributeText("name")(source).getOrElse(""), toAttributeText("url")(source).getOrElse(""),
        toAttributeText("username")(source).getOrElse(""), toAttributeText("password")(source).getOrElse(""))
    }

    (dolphin \\ "transforms" \\ "transform").map { node =>
      val childs = nodeToSubTransform(Some(sources))(node)
      nodeToTransform(Some(sources), childs)(node)
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

  private[this] def nodeToTransform(dbs: Option[Seq[DBSourceModel]] = None, childs: Option[TransformModel]): PartialFunction[Node, TransformModel] = {
    case node =>
      val columns = nodeToColumns(node)

      TransformModel(toAttributeText("from")(node).getOrElse(""), toAttributeText("to")(node).getOrElse(""),
        toAttributeText("fromPath")(node), toAttributeText("toPath")(node), toAttributeText("format")(node),
        toAttributeText("table")(node), toAttributeText("sql")(node), toAttributeText("tmpTable")(node),
        toAttributeText("hiveTable")(node), toAttributeText("mode")(node), toAttributeText("process")(node),
        toAttributeText("cache")(node).map(_.toBoolean), dbs, columns, childs)
  }

  private[this] def nodeToSubTransform(dbs: Option[Seq[DBSourceModel]] = None): PartialFunction[Node, Option[TransformModel]] = {
    case node =>
      node.child.find(_.label == "subtransform").headOption.map { nodeToTransform(dbs, None) }
  }

  private[this] def toAttributeText(name: String): PartialFunction[Node, Option[String]] = {
    case node => node.attribute(name).flatMap(_.headOption).map(_.text)
  }

}