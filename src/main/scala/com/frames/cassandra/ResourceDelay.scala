package com.frames.cassandra
import cats.effect.{Resource, Sync}

import scala.util.{Failure, Success, Try}

trait ResourceDelay {

  def withResourceDelay[F[_], A, B](
      block: A => B
  )(pf: PartialFunction[Throwable, OperationError] = PartialFunction.empty)(implicit sync: Sync[F], resource: Resource[F, A]): ErrorOr[F, B] =
    ErrorOr(
      resource
        .use(
          session =>
            sync
              .delay(Try(block(session)) match {
                case Failure(ex)    => Left(pf.applyOrElse(ex, (_: Throwable) => CustomError(ex.getMessage)))
                case Success(value) => Right(value)
              })
        )
    )

}
