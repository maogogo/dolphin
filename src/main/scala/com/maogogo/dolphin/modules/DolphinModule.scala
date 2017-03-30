package com.maogogo.dolphin.modules

import com.maogogo.dolphin.models._
import scala.xml._

trait DolphinModule { self =>

  def provideDolphinModel(file: String): Seq[TransformModel] = {

    val dolphin = XML.load(file)

    val sources = (dolphin \\ "dolphin" \\ "dbsources" \\ "source").map { source =>
      DBSourceModel(toAttributeText("name")(source).getOrElse(""), toAttributeText("url")(source).getOrElse(""),
        toAttributeText("username")(source).getOrElse(""), toAttributeText("password")(source).getOrElse(""))
    }

    (dolphin \\ "transforms" \\ "transform").map { tran =>

      val columns = tran.child.find(_.label == "columns").headOption.map {
        _.child.filter(_.label == "column").map { node =>
          val nullable = toAttributeText("nullable")(node).getOrElse("") match {
            case s if (s.toLowerCase() == "true") || s == "1" => true
            case _ => false
          }

          ColumnModel(toAttributeText("name")(node).getOrElse(""), toAttributeText("cname")(node),
            toAttributeText("type")(node), nullable)
        }
      }

      val child = tran.child.find(_.label == "transform").headOption.map { node =>

        //这里面应该有个 columns
        TransformModel(toAttributeText("from")(node).getOrElse(""), toAttributeText("to")(node).getOrElse(""),
          toAttributeText("fromPath")(node), toAttributeText("toPath")(node), toAttributeText("format")(node),
          toAttributeText("table")(node), toAttributeText("sql")(node), toAttributeText("tmpTable")(node),
          toAttributeText("hiveTable")(node), Some(sources), None, None)
      }

      TransformModel(toAttributeText("from")(tran).getOrElse(""), toAttributeText("to")(tran).getOrElse(""),
        toAttributeText("fromPath")(tran), toAttributeText("toPath")(tran), toAttributeText("format")(tran),
        toAttributeText("table")(tran), toAttributeText("sql")(tran), toAttributeText("tmpTable")(tran),
        toAttributeText("hiveTable")(tran), Some(sources), columns, child)
    }

    //DolphinModel(sources, trans)
  }

  //  private[this] def toTransformModel: PartialFunction[Node, TransformModel] = {
  //    case tran => TransformModel(toAttributeText("from")(tran).getOrElse(""), toAttributeText("to")(tran).getOrElse(""),
  //        toAttributeText("fromPath")(tran), toAttributeText("toPath")(tran), toAttributeText("format")(tran),
  //        toAttributeText("table")(tran), toAttributeText("sql")(tran), toAttributeText("tmpTable")(tran),
  //        toAttributeText("hiveTable")(tran), Some(sources), columns)
  //  }

  private[this] def toAttributeText(name: String): PartialFunction[Node, Option[String]] = {
    case node => node.attribute(name).flatMap(_.headOption).map(_.text)
  }

}