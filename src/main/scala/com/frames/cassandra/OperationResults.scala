package com.frames.cassandra

sealed trait OperationResult

case object OK                extends OperationResult
case object KeyspaceCreated   extends OperationResult
case object FrameTableCreated extends OperationResult
case object FrameCreated      extends OperationResult
case object QueryExecuted     extends OperationResult
case object FramesConsistent  extends OperationResult
