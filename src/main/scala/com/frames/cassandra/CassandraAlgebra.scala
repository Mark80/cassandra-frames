package com.frames.cassandra

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import com.datastax.driver.core.Session

import scala.collection.JavaConverters._

sealed trait OperationResult
sealed trait Error

case object OK                extends OperationResult
case object KeyspaceCreated   extends OperationResult
case object FrameTableCreated extends OperationResult

case class CustomError(msg: String)                                       extends Error
case class KeyspaceAlreadyExists(msg: String = "Keyspace already exists") extends Error

object ErrorOr {
  def apply[F[_], A](value: F[Either[Error, A]]) = EitherT.apply(value)
}

object CassandraAlgebra {

  type ErrorOr[F[_], A] = EitherT[F, Error, A]

  def createFrameTable[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    ErrorOr(withSessionDelay { session =>
      session.execute(InitializationOps.createFrameTable(keySpace))
    }.map(_ => Right(FrameTableCreated: OperationResult)))

  def createKeyspace[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    ErrorOr(withSessionDelay { session =>
      session
        .execute(InitializationOps.createKeyspace(keySpace))
    }.map(_ => Right(OK: OperationResult)))

  def getLastScriptApplied[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, Option[AppliedScript]] =
    ErrorOr(withSessionDelay { session =>
      Right(
        session
          .execute(FramesOps.getLastAppliedScript(keySpace))
          .iterator()
          .asScala
          .toList
          .headOption
          .map(FramesOps.toAppliedScript)
      )
    })

  private def withSessionDelay[F[_], A](block: Session => A)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): F[A] =
    sessionResource
      .use(
        session =>
          sync
            .delay(block(session))
      )
}
