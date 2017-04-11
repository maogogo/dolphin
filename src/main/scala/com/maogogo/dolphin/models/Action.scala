package com.maogogo.dolphin.models

sealed trait Action

case class MovePath(from: String, to: String, deleteSource: Boolean) extends Action

case class MergeFile(from: String, to: String, deleteSource: Boolean) extends Action

case class RemovePath(target: String) extends Action

case class GetMergeFile(from: String, to: String, deleteSource: Boolean) extends Action

case class OtherAction(from: String, to: String) extends Action