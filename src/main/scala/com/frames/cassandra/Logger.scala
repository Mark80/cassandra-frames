package com.frames.cassandra

import cats.effect.IO

trait Logger[F[_]] {

  def info(message: => String): F[Unit]
  def error(message: => String): F[Unit]
  def debug(message: => String): F[Unit]

}

object Logger {

  def info[F[_]](message: String)(implicit logger: Logger[F]): F[Unit] =
    logger.info(message)

  implicit val ioLogger = new Logger[IO] {

    import com.typesafe.scalalogging.{Logger => ScalaLogger}
    private val log = ScalaLogger("Logger")

    def info(message: => String): IO[Unit] =
      IO(log.info(message))

    def error(message: => String): IO[Unit] =
      IO(log.error(message))

    def debug(message: => String): IO[Unit] =
      IO(log.debug(message))

  }

}
