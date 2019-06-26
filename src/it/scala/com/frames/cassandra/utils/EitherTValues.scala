package com.frames.cassandra.utils

import cats.data.EitherT
import cats.effect.IO
import org.scalactic.source
import org.scalatest.exceptions.{StackDepthException, TestFailedException}

trait EitherTValues {

  implicit class EitherValuable[+E, +T](eitherT: EitherT[IO, E, T]) {

    def rightValue(implicit pos: source.Position): T =
      try {
        val result: Either[E, T] = eitherT.value.unsafeRunSync()
        result.right.get
      } catch {
        case cause: NoSuchElementException =>
          throw new TestFailedException((_: StackDepthException) => Some("Either value is a Left"), Some(cause), pos)
      }

    def leftValue(implicit pos: source.Position): E =
      try {
        val result: Either[E, T] = eitherT.value.unsafeRunSync()
        result.left.get
      } catch {
        case cause: NoSuchElementException =>
          throw new TestFailedException((_: StackDepthException) => Some("Either value is a Right"), Some(cause), pos)
      }

  }

}
