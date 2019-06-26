package com.frames.cassandra

import cats.data.EitherT
import cats.effect.IO
import com.frames.cassandra.utils.EitherTValues
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{Matchers, WordSpec}

class EitherTValuesExample extends WordSpec with Matchers with EitherTValues {

  "EitherTValues" should {

    "return the right projection" in {
      val eitherT = EitherT.apply[IO, Nothing, String](IO(Right("result")))
      eitherT.rightValue shouldBe "result"
    }

    "fail if right value is call on left" in {
      val eitherT = EitherT.apply[IO, String, String](IO(Left("result")))
      a[TestFailedException] should be thrownBy eitherT.rightValue
    }

    "return the left projection" in {
      val eitherT = EitherT.apply[IO, String, Nothing](IO(Left("result")))
      eitherT.leftValue shouldBe "result"
    }

    "fail if left value is call on right" in {
      val eitherT = EitherT.apply[IO, String, String](IO(Right("result")))
      a[TestFailedException] should be thrownBy eitherT.leftValue
    }

  }

}
