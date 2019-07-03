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

        val result = ScriptsOps
          .loadScripts[IO](notExistingFolder)
          .leftValue

        result shouldBe ScriptFolderNotExists(notExistingFolder)
      }

      "folder is empty" in {

        ScriptsOps
          .loadScripts[IO](customEmptyFolder)
          .rightValue shouldBe empty
      }

      "folder not contains files with .cql extensions " in {

        ScriptsOps
          .loadScripts[IO](folderWithoutCql)
          .rightValue shouldBe empty
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
          mockAppliedScript(1, "V1_script_name.cql", FramesOps.md5("ADD TABLE1"), success = true, None),
          mockAppliedScript(2, "V2_script_name.cql", FramesOps.md5("ADD TABLE2"), success = true, None)
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
            mockAppliedScript(1, "V1_script_name.cql", FramesOps.md5("ADD TABLE1"), success = true, None),
            mockAppliedScript(2, "V2_script_name.cql", FramesOps.md5("ADD TABLE2"), success = true, None)
          )

        val result = for {
          scripts <- ScriptsOps.loadScripts[IO]()
          res     <- ScriptsOps.getScriptWithChangedSource[IO](scripts, applied)
        } yield res

        result.rightValue shouldBe empty
      }

      "no files are previously applied" in {

        val result = for {
          scripts <- ScriptsOps.loadScripts[IO]()
          res     <- ScriptsOps.getScriptWithChangedSource[IO](scripts, Nil)
        } yield res

        result.rightValue shouldBe empty
      }
    }

    "splitScriptSource" when {
      "split in single queries the source body event with char separator inside line and script on multiple line" in {
        val result = for {
          scripts <- ScriptsOps.loadScripts[IO](Config.DefaultScriptFolder)
          queries <- ScriptsOps.splitScriptSource[IO](scripts)
        } yield queries

        val mValue = result.rightValue
        mValue("V1_script_name.cql") should have size 1
        mValue("V3_script_with_separator.cql") should have size 5
        mValue("V3_script_with_separator.cql")(3) shouldBe "-- last insert\nINSERT INTO\ntable\n(c1, c2, c3)\nvalues\n('qwe; & kdij',\n 1,\n 'othe;era')"
      }
    }
  }

}
