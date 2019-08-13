package com.frames
import cats.Functor
import cats.data.EitherT

package object cassandra {

  type ErrorOr[F[_], A] = EitherT[F, OperationError, A]

  object ErrorOr {

    def liftF[F[_], A](value: F[A])(implicit F: Functor[F]): ErrorOr[F, A] = EitherT.liftF(value)

    def apply[F[_], A](value: F[Either[OperationError, A]]) = EitherT.apply(value)
  }

}
