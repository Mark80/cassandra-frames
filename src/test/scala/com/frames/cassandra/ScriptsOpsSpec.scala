package com.frames.cassandra

import cats.effect.{IO, Resource}
import org.scalatest.{Matchers, WordSpec}

class ScriptsOpsSpec extends WordSpec with Matchers {

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
          .use(res => IO(res))
          .unsafeRunSync() shouldBe Nil
      }

      "folder is empty" in {

        ScriptsOps
          .loadScripts[IO](customEmptyFolder)
          .use(res => IO(res))
          .unsafeRunSync() shouldBe Nil
      }

      "folder not contains files with .cql extensions " in {

        ScriptsOps
          .loadScripts[IO](folderWithoutCql)
          .use(res => IO(res))
          .unsafeRunSync() shouldBe Nil
      }
    }

    "return list of sources" when {

      "default folder contains files with .cql extensions " in {

        ScriptsOps
          .loadScripts[IO]()
          .use(res => IO(res))
          .unsafeRunSync() should have size 2
      }

      "custom folder contains files with .cql extensions " in {

        ScriptsOps
          .loadScripts[IO](customNotEmptyFolder)
          .use(res => IO(res))
          .unsafeRunSync() should have size 2
      }
    }

    "return list of scripts with checksum different" in {

      val applied =
        List(AppliedScript("V1_script_name.cql", ScriptsOps.md5("ADD TABLE1")), AppliedScript("V2_script_name.cql", ScriptsOps.md5("ADD TABLE2")))

      val result = for {
        scripts <- ScriptsOps.loadScripts[IO](withChecksumDifference)
        res     <- Resource.liftF(ScriptsOps.getVariationInScriptResources[IO](scripts, applied))
      } yield res

      result.use(resources => IO(resources)).unsafeRunSync() should have size 1
    }

    "return empty list" when {
      "checksum are correct" in {

        val applied =
          List(AppliedScript("V1_script_name.cql", ScriptsOps.md5("ADD TABLE1")), AppliedScript("V2_script_name.cql", ScriptsOps.md5("ADD TABLE2")))

        val result = for {
          scripts <- ScriptsOps.loadScripts[IO]()
          res     <- Resource.liftF(ScriptsOps.getVariationInScriptResources[IO](scripts, applied))
        } yield res

        result.use(resources => IO(resources)).unsafeRunSync() shouldBe Nil
      }

      "no files are previously applied" in {

        val result = for {
          scripts <- ScriptsOps.loadScripts[IO]()
          res     <- Resource.liftF(ScriptsOps.getVariationInScriptResources[IO](scripts, Nil))
        } yield res

        result.use(resources => IO(resources)).unsafeRunSync() shouldBe Nil
      }
    }
  }

}
