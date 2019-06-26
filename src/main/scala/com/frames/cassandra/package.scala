package com.frames
import cats.data.EitherT

package object cassandra {

  type ErrorOr[F[_], A] = EitherT[F, OperationError, A]

  object ErrorOr {
    def apply[F[_], A](value: F[Either[OperationError, A]]) = EitherT.apply(value)
  }

}
