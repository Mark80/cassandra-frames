package com.frames.cassandra

import cats.effect._
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.AlreadyExistsException

import scala.collection.JavaConverters._

object CassandraAlgebra extends ResourceDelay {

  def createFrameTable[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    useResourceWithDelay[F, Session, OperationResult] { session =>
      session.execute(InitializationOps.createFrameTable(keySpace))
      FrameTableCreated
    } {
      case _: AlreadyExistsException => FrameTableAlreadyExists
    }

  def createKeyspace[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    useResourceWithDelay[F, Session, OperationResult] { session =>
      session
        .execute(InitializationOps.createKeyspace(keySpace))
      KeyspaceCreated
    } {
      case _: AlreadyExistsException => KeyspaceAlreadyExists
    }

  def getExecutedScripts[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, List[ExecutedScript]] =
    useResourceWithDelay[F, Session, List[ExecutedScript]] { session =>
      session
        .execute(FramesOps.getExecutedScripts(keySpace))
        .iterator()
        .asScala
        .toList
        .map(FramesOps.toExecutedScript)
    }()

  def getLastSuccessfulExecutedScript[F[_]](
      keySpace: String
  )(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, Option[ExecutedScript]] =
    useResourceWithDelay[F, Session, Option[ExecutedScript]] { session =>
      session
        .execute(FramesOps.getSuccessfulExecutedScripts(keySpace))
        .iterator()
        .asScala
        .toList
        .headOption
        .map(FramesOps.toExecutedScript)
    }()

  def executeQuery[F[_]](query: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    useResourceWithDelay[F, Session, OperationResult] { session =>
      session.execute(query)
      QueryExecuted
    }()

  def insertExecutedScript[F[_]](
      keySpace: String,
      executedScript: ExecutedScript
  )(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    useResourceWithDelay[F, Session, OperationResult] { session =>
      session.execute(FramesOps.boundInsertStatement(FramesOps.insertStatement(keySpace, !executedScript.success), executedScript)(session))
      FrameCreated
    }()
}
