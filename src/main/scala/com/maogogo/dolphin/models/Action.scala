package com.maogogo.dolphin.models

sealed trait HadoopSource extends FromSource {
  val path = ""
  val tmpTable: Option[String] = None
  val params: Map[String, String] = Map.empty
}

case class ActionMovePath(from: String, to: String, deleteSource: Boolean) extends HadoopSource

case class ActionMergeFile(from: String, to: String, deleteSource: Boolean) extends HadoopSource

case class ActionRemovePath(target: String) extends HadoopSource

case class ActionGetMergeFile(from: String, to: String, local: String, deleteSource: Boolean) extends HadoopSource

case class OtherAction(from: String, to: String) extends HadoopSource