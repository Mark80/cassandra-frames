package com.frames.cassandra

import cats.effect.IO
import com.datastax.driver.core.ResultSet
import org.scalatest.OptionValues

class CassandraAlgebraSpec extends CassandraBaseSpec with OptionValues {

  val frameTable = "frames_table"

  override def tables: List[String] = List(frameTable)

  override def keySpace: String = "keyspace_name"

  import CassandraAlgebra._

  "CassandraAlgebra" when {

    "keyspace not exists" should {

      "return Created" in {

        val (operationResult, keySpaceCreated) = (for {
          result        <- createKeyspace[IO](keySpace)
          maybeKeyspace <- checkKeySpace()
        } yield (result, maybeKeyspace)).unsafeRunSync()

        operationResult shouldBe OK
        keySpaceCreated shouldBe true
      }

    }

    "keyspace exists" should {

      "return Exists" in {

        (for {
          _      <- createKeyspace[IO](keySpace)
          result <- createKeyspace[IO](keySpace)
        } yield result)
          .unsafeRunSync() shouldBe KeyspaceAlreadyExists
      }
    }

    "exception occur" should {

      "return Failed with query syntax error message" in {

        createKeyspace[IO]("£££%_").unsafeRunSync() shouldBe Error("line 1:16 no viable alternative at character '£'")
      }
    }

    "schema table not exist" should {
      "create schema table" in {

        val (createResult, check) = (for {
          _      <- createKeyspace[IO](keySpace)
          result <- createFrameTable[IO](keySpace)
          check  <- checkTable(keySpace, frameTable)
        } yield (result, check))
          .unsafeRunSync()

        createResult shouldBe FrameTableCreated
        check shouldBe true

      }

    }

    "schema table is empty" should {
      "return None" in {

        val lastScriptApplied = (for {
          _                 <- createKeyspace[IO](keySpace)
          _                 <- createFrameTable[IO](keySpace)
          lastScriptApplied <- getLastScriptApplied[IO](keySpace)
        } yield lastScriptApplied)
          .unsafeRunSync()

        lastScriptApplied shouldBe None
      }

      "return last success applied script" in {

        val lastScriptApplied = (for {
          _                 <- createKeyspace[IO](keySpace)
          _                 <- createFrameTable[IO](keySpace)
          _                 <- insertTestRecord(1, success = true, None)
          _                 <- insertTestRecord(2, success = true, None)
          _                 <- insertTestRecord(3, success = false, Some("error"))
          lastScriptApplied <- getLastScriptApplied[IO](keySpace)
        } yield lastScriptApplied)
          .unsafeRunSync()

        lastScriptApplied.value.version shouldBe 2
      }
    }
  }

  private def insertTestRecord(version: Long, success: Boolean, messageError: Option[String]): IO[ResultSet] =
    sessionResource.use(session => IO(session.execute(s"""INSERT INTO $keySpace.frames_table
                       (version, file_name, checksum, date, error_message, success, execution_time)
                       VALUES
                       ($version, 'V${version}_script_name.cql', '${ScriptsOps.md5(s"SCRIPT BODY $version")}',
                        '2019-01-01', '${messageError.getOrElse("")}', $success, 10);""")))

  private def checkKeySpace(): IO[Boolean] =
    clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keySpace)).isDefined))

  private def checkTable(keyspace: String, table: String): IO[Boolean] =
    clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keyspace).getTable(table)).isDefined))
}
