package com.frames.cassandra

import cats.data.State
import org.scalatest.{Matchers, WordSpec}

class LoggerSpec extends WordSpec with Matchers {

  type StateLogger[A] = State[String, A]

  implicit val testLogger: Logger[StateLogger] = new Logger[StateLogger] {

    private def newState(message: String): StateLogger[Unit] =
      State(s => (s + message, Unit))

    def info(message: => String): StateLogger[Unit] =
      newState(message)

    def error(message: => String): StateLogger[Unit] =
      newState(message)

    def debug(message: => String): StateLogger[Unit] =
      newState(message)
  }

  "Logger" should {

    "log messages" in {

      val message1 = "first message"
      val message2 = "second message"

      val log = for {
        _ <- Logger.info[StateLogger](message1)
        _ <- Logger.info[StateLogger](message2)
      } yield ()

      log.run("").value._1 shouldBe message1 + message2

    }

  }

}
