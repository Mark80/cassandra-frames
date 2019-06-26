package com.frames.cassandra
import cats.data.EitherT
import cats.effect.{Resource, Sync}

import scala.util.{Failure, Success, Try}

sealed trait OperationResult
sealed trait Error

case object OK                extends OperationResult
case object KeyspaceCreated   extends OperationResult
case object FrameTableCreated extends OperationResult

case object KeyspaceAlreadyExists   extends Error
case object FrameTableAlreadyExists extends Error
case class CustomError(msg: String) extends Error

object ErrorOr {
  def apply[F[_], A](value: F[Either[Error, A]]) = EitherT.apply(value)
}

trait ResourceDelay {

  type ErrorOr[F[_], A] = EitherT[F, Error, A]

  def withResourceDelay[F[_], A, B](
      block: A => B
  )(pf: PartialFunction[Throwable, Error] = PartialFunction.empty)(implicit sync: Sync[F], resource: Resource[F, A]): ErrorOr[F, B] =
    ErrorOr(
      resource
        .use(
          session =>
            sync
              .delay(Try(block(session)) match {
                case Failure(ex) =>
                  Left(
                    if (pf.isDefinedAt(ex)) pf(ex)
                    else CustomError(ex.getMessage)
                  )
                case Success(value) => Right(value)
              })
        )
    )

}
