package com.frames.cassandra

sealed trait OperationResult

case object OK                extends OperationResult
case object KeyspaceCreated   extends OperationResult
case object FrameTableCreated extends OperationResult
