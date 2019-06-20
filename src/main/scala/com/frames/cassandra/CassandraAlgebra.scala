package com.frames.cassandra

import cats.effect._
import cats.implicits._
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.AlreadyExistsException

sealed trait OperationResult

case object OK                extends OperationResult
case class Error(msg: String) extends OperationResult

case object KeyspaceCreated       extends OperationResult
case object KeyspaceAlreadyExists extends OperationResult
case object FrameTableCreated     extends OperationResult

object CassandraAlgebra {

  def createFrameTable[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): F[OperationResult] =
    sessionResource
      .use(
        session =>
          sync
            .delay(session.execute(InitializationOps.createFrameTable(keySpace)))
            .map(_ => FrameTableCreated: OperationResult)
      )

  def createKeyspace[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): F[OperationResult] =
    sessionResource
      .use(
        session =>
          sync
            .delay(session.execute(InitializationOps.createKeyspace(keySpace)))
            .map(_ => OK: OperationResult)
            .handleErrorWith {
              case _: AlreadyExistsException => sync.delay(KeyspaceAlreadyExists)
              case qe: Throwable             => sync.pure(Error(qe.getMessage))
            }
      )
}
