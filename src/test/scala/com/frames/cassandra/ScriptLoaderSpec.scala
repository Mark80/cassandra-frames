package com.frames.cassandra

import cats.effect.IO
import org.scalatest.{Matchers, WordSpec}

class ScriptLoaderSpec extends WordSpec with Matchers {

  "LoadScripts" should {

    val notExistingFolder = "/not-existing-folder"
    val customEmptyFolder = "/emptyFolder"
    val defaultFolder = "/migration/scripts"
    val folderWithoutCql = "/folderWithoutCql"

    "return empty list" when {

      "folder not exists" in {

        ScriptLoader.loadScripts[IO](notExistingFolder).use(res => IO(res)).unsafeRunSync() shouldBe Nil
      }

      "folder is empty" in {

        ScriptLoader.loadScripts[IO](customEmptyFolder).use(res => IO(res)).unsafeRunSync() shouldBe Nil
      }

      "folder not contains files with .cql extensions " in {

        ScriptLoader.loadScripts[IO](folderWithoutCql).use(res => IO(res)).unsafeRunSync() shouldBe Nil
      }
    }

    "return list of sources" when {

      "folder contains files with .cql extensions " in {

        ScriptLoader.loadScripts[IO](defaultFolder).use(res => IO(res)).unsafeRunSync() should have size 2
      }
    }
  }
}
