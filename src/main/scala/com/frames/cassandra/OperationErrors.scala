package com.frames.cassandra

sealed trait OperationError

case object ScriptsLoadingFailed    extends OperationError
case object KeyspaceAlreadyExists   extends OperationError
case object FrameTableAlreadyExists extends OperationError
case object ScriptFolderNotExists   extends OperationError
case class CustomError(msg: String) extends OperationError
