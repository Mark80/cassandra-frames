package com.frames.cassandra

import org.scalatest.{Matchers, WordSpec}

class ExecutedScriptSpec extends WordSpec with Matchers with FixedClock {

  "ExecutedScript" should {

    "create from cqlfile" in {

      val cqlFile = CqlFile("V1_script", "body")

      val expectedExecutedScript =
        ExecutedScript(1L, "V1_script", "checksum", "2019-01-01", None, true, 100L)

      val executedScript = ExecutedScript.from(cqlFile, "checksum", "2019-01-01", None, 100L)

      executedScript shouldBe expectedExecutedScript
    }

  }

}
