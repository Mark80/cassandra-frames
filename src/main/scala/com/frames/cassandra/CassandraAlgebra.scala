package com.frames.cassandra

import cats.effect._
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.AlreadyExistsException

import scala.collection.JavaConverters._

object CassandraAlgebra extends ResourceDelay {

  def createFrameTable[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    withResourceDelay[F, Session, OperationResult] { session =>
      session.execute(InitializationOps.createFrameTable(keySpace))
      FrameTableCreated
    } {
      case _: AlreadyExistsException => FrameTableAlreadyExists
    }

  def createKeyspace[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    withResourceDelay[F, Session, OperationResult] { session =>
      session
        .execute(InitializationOps.createKeyspace(keySpace))
      KeyspaceCreated
    } {
      case _: AlreadyExistsException => KeyspaceAlreadyExists
    }

  def getLastScriptApplied[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, Option[AppliedScript]] =
    withResourceDelay[F, Session, Option[AppliedScript]] { session =>
      session
        .execute(FramesOps.getAppliedScripts(keySpace))
        .iterator()
        .asScala
        .toList
        .headOption
        .map(FramesOps.toAppliedScript)
    }()

  def insertAppliedScript[F[_]](
      keySpace: String,
      appliedScript: AppliedScript
  )(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    withResourceDelay[F, Session, OperationResult] { session =>
      session.execute(FramesOps.boundInsertStatement(FramesOps.insertStatement(keySpace, appliedScript.success), appliedScript)(session))
      ScriptApplied
    }()
}
