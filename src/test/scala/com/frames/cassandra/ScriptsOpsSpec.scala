package com.frames.cassandra

import cats.effect.IO
import com.frames.cassandra.utils.EitherTValues
import org.scalatest.{Matchers, WordSpec}

class ScriptsOpsSpec extends WordSpec with Matchers with AlgebraFixture with EitherTValues {

  "ScriptsOpsSpec" should {

    val notExistingFolder      = "/not-existing-folder"
    val customEmptyFolder      = "/emptyFolder"
    val customNotEmptyFolder   = "/customNotEmptyFolder"
    val folderWithoutCql       = "/folderWithoutCql"
    val withChecksumDifference = "/withChecksumDifference"

    "return empty list" when {

      "folder not exists" in {

        ScriptsOps
          .loadScripts[IO](notExistingFolder)
          .rightValue shouldBe Nil
      }

      "folder is empty" in {

        ScriptsOps
          .loadScripts[IO](customEmptyFolder)
          .rightValue shouldBe Nil
      }

      "folder not contains files with .cql extensions " in {

        ScriptsOps
          .loadScripts[IO](folderWithoutCql)
          .rightValue shouldBe Nil
      }
    }

    "return list of sources" when {

      "default folder contains files with .cql extensions " in {

        ScriptsOps
          .loadScripts[IO]()
          .rightValue should have size 3
      }

      "custom folder contains files with .cql extensions " in {

        ScriptsOps
          .loadScripts[IO](customNotEmptyFolder)
          .rightValue should have size 2
      }
    }

    "return list of scripts with checksum different" in {

      val applied =
        List(
          mockAppliedScript(1, "V1_script_name.cql", ScriptsOps.md5("ADD TABLE1"), success = true, None),
          mockAppliedScript(2, "V2_script_name.cql", ScriptsOps.md5("ADD TABLE2"), success = true, None)
        )

      val result = for {
        scripts <- ScriptsOps.loadScripts[IO](withChecksumDifference)
        res     <- ScriptsOps.getScriptWithChangedSource[IO](scripts, applied)
      } yield res

      result.rightValue should have size 1
    }

    "return empty list" when {
      "checksum are correct" in {

        val applied =
          List(
            mockAppliedScript(1, "V1_script_name.cql", ScriptsOps.md5("ADD TABLE1"), success = true, None),
            mockAppliedScript(2, "V2_script_name.cql", ScriptsOps.md5("ADD TABLE2"), success = true, None)
          )

        val result = for {
          scripts <- ScriptsOps.loadScripts[IO]()
          res     <- ScriptsOps.getScriptWithChangedSource[IO](scripts, applied)
        } yield res

        result.rightValue shouldBe Nil
      }

      "no files are previously applied" in {

        val result = for {
          scripts <- ScriptsOps.loadScripts[IO]()
          res     <- ScriptsOps.getScriptWithChangedSource[IO](scripts, Nil)
        } yield res

        result.rightValue shouldBe Nil
      }
    }

    "splitScriptSource" should {
      "split in single queries the source body event with char separator inside line and script on multiple line" in {
        val result = for {
          scripts <- ScriptsOps.loadScripts[IO](Config.DefaultScriptFolder)
          queries <- ScriptsOps.splitScriptSource[IO](scripts)
        } yield queries

        val map: Map[String, List[String]] = result.rightValue
        map("V1_script_name.cql") should have size 1
        map("V3_script_with_separator.cql") should have size 4
      }
    }
  }

}
