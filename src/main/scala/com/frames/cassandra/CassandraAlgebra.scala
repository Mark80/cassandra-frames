package com.frames.cassandra

import cats.effect._
import cats.implicits._
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.AlreadyExistsException
import scala.collection.JavaConverters._

sealed trait OperationResult

case object OK                extends OperationResult
case class Error(msg: String) extends OperationResult

case object KeyspaceCreated       extends OperationResult
case object KeyspaceAlreadyExists extends OperationResult
case object FrameTableCreated     extends OperationResult

object CassandraAlgebra {

  def createFrameTable[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): F[OperationResult] =
    withSessionDelay { session =>
      session.execute(InitializationOps.createFrameTable(keySpace))
    }.map(_ => FrameTableCreated: OperationResult)

  def createKeyspace[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): F[OperationResult] =
    withSessionDelay { session =>
      session
        .execute(InitializationOps.createKeyspace(keySpace))
    }.map(_ => OK: OperationResult).handleErrorWith {
      case _: AlreadyExistsException => sync.delay(KeyspaceAlreadyExists)
      case qe: Throwable             => sync.pure(Error(qe.getMessage))
    }

  def getLastScriptApplied[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): F[Option[AppliedScript]] =
    withSessionDelay { session =>
      session
        .execute(FramesOps.getLastAppliedScript(keySpace))
        .iterator()
        .asScala
        .toList
        .map(FramesOps.toAppliedScript)
        .headOption
    }

  private def withSessionDelay[F[_], A](block: Session => A)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): F[A] =
    sessionResource
      .use(
        session =>
          sync
            .delay(block(session))
      )
}
