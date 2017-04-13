package com.maogogo.dolphin.services

import java.io.File
import freemarker.template.Configuration
import java.text.SimpleDateFormat
import com.typesafe.config.Config
import java.util.Date
import java.io.PrintWriter
import org.apache.commons.io.FilenameUtils
import scala.collection.JavaConversions._
import freemarker.cache.StringTemplateLoader
import java.io.StringWriter
import scala.collection.JavaConverters._

trait TemplateService {

  def createTemplate(implicit config: Config): (String, Map[String, String]) = {
    val input = config.getString("input")
    val tpl = config.getString("tpl_path")
    val params = config.getObject("params").unwrapped.map { kv =>
      (kv._1 -> String.valueOf(kv._2))
    }.toMap

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = sdf.format(new Date)

    //TODO file exists
    val file = new File(input)
    val fileNm = FilenameUtils.getName(file.getName)
    val fileExt = FilenameUtils.getExtension(file.getName)
    val filePath = FilenameUtils.getFullPath(file.getName)
    println("filepath => " + filePath)
    val path = s"${tpl}${File.separator}${fileNm}_${date}.${fileExt}"

    val configuration = new Configuration()
    val t = configuration.getTemplate(input)
    val outFile = new File(path)
    val pw = new PrintWriter(outFile)
    t.process(params, pw)

    (path, params)
  }

  def getContext(context: String, params: Map[String, String]): String = {
    val configuration = new Configuration
    val stringLoader = new StringTemplateLoader
    stringLoader.putTemplate("myTemplate", context)
    configuration.setTemplateLoader(stringLoader)
    val t = configuration.getTemplate("myTemplate", "utf-8")
    val writer = new StringWriter
    t.process(params.asJava, writer)
    writer.toString
  }

  def checkFilePath(tpl: String): Unit = {
    val file = new File(tpl)
    file.isDirectory match {
      case true =>
      //Log.debug("check the template path success")
      case _ =>
        //Log.info("the template directory is not exists, auto create the path")
        val flag = file.mkdirs()
      //Log.info(s"create the template directory : $flag")
    }
  }
}

object TemplateService extends TemplateService