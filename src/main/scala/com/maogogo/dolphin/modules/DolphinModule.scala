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

      val columns = tran.child.filter(_.label == "column").map { node =>
        val nullable = toAttributeText("nullable")(node).getOrElse("true").toBoolean
        ColumnModel(toAttributeText("name")(node).getOrElse(""), toAttributeText("cname")(node),
          toAttributeText("type")(node), nullable)
      }

      TransformModel(toAttributeText("from")(tran).getOrElse(""), toAttributeText("to")(tran).getOrElse(""),
        toAttributeText("fromPath")(tran), toAttributeText("toPath")(tran), toAttributeText("format")(tran),
        toAttributeText("table")(tran), toAttributeText("sql")(tran), toAttributeText("tmpTable")(tran),
        toAttributeText("hiveTable")(tran), Some(sources), Some(columns))
    }

    //DolphinModel(sources, trans)
  }

  private[this] def toAttributeText(name: String): PartialFunction[Node, Option[String]] = {
    case node => node.attribute(name).flatMap(_.headOption).map(_.text)
  }

}