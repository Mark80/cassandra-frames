package com.frames.cassandra

import cats.effect.IO
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class ScriptsOpsSpec extends WordSpec with Matchers {

  "ScriptsOpsSpec" should {

    val notExistingFolder    = "/not-existing-folder"
    val customEmptyFolder    = "/emptyFolder"
    val customNotEmptyFolder = "/customNotEmptyFolder"
    val folderWithoutCql     = "/folderWithoutCql"

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

      val scripts = List(("1-add-table.cql", Source.fromString("ADD TABLE1")), ("2-add-table.cql", Source.fromString("2")))
      val applied = List(
        ("1-add-table.cql", ScriptsOps.md5("ADD TABLE1")),
        ("2-add-table.cql", ScriptsOps.md5("ADD TABLE2")),
        ("3-remove-table.cql", ScriptsOps.md5("REMOVE TABLE"))
      )

      ScriptsOps
        .compareAppliedScriptsWithSources[IO](scripts, applied)
        .unsafeRunSync() should have size 2
    }

    "return empty list" when {
      "checksum are correct" in {

        val scripts = List(
          ("1-add-table.cql", Source.fromString("ADD TABLE1")),
          ("2-add-table.cql", Source.fromString("ADD TABLE2")),
          ("3-remove-table.cql", Source.fromString("REMOVE TABLE"))
        )
        val applied = List(("1-add-table.cql", ScriptsOps.md5("ADD TABLE1")), ("2-add-table.cql", ScriptsOps.md5("ADD TABLE2")))

        ScriptsOps
          .compareAppliedScriptsWithSources[IO](scripts, applied)
          .unsafeRunSync() shouldBe Nil
      }

      "no files are previously applied" in {

        val scripts = List(
          ("1-add-table.cql", Source.fromString("ADD TABLE1")),
          ("2-add-table.cql", Source.fromString("ADD TABLE2")),
          ("3-remove-table.cql", Source.fromString("REMOVE TABLE"))
        )
        ScriptsOps
          .compareAppliedScriptsWithSources[IO](scripts, Nil)
          .unsafeRunSync() shouldBe Nil
      }
    }
  }

}
