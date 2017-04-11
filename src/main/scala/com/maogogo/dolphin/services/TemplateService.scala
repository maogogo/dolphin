package com.maogogo.dolphin.services

import java.io.File
import freemarker.template.Configuration
import java.text.SimpleDateFormat
import com.typesafe.config.Config
import java.util.Date
import java.io.PrintWriter
import org.apache.commons.io.FilenameUtils

trait TemplateService {

  def createTemplate(implicit config: Config): String = {
    val input = config.getString("input")
    val tpl = config.getString("tpl_path")
    val params = config.getObject("params").unwrapped()

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
    
    path
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