package com.frames.cassandra

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import cats.data.EitherT
import cats.effect.IO
import com.frames.cassandra.utils.EitherTValues
import org.scalatest.OptionValues

class CassandraAlgebraSpec extends CassandraBaseSpec with OptionValues with EitherTValues {

  val frameTable = "frames_table"

  override def tables: List[String] = List(frameTable)

  override def keySpace: String = "keyspace_name"

  import CassandraAlgebra._
  import ScriptsOps._

  "CassandraAlgebra" when {

    "keyspace not exists" should {

      "return KeyspaceCreated" in {

        val keyspaceCreated = for {
          result        <- createKeyspace[IO](keySpace)
          maybeKeyspace <- checkKeySpace()
        } yield (result, maybeKeyspace)

        keyspaceCreated.rightValue shouldBe (KeyspaceCreated, true)
      }

    }

    "keyspace exists" should {

      "return KeyspaceAlreadyExists" in {

        val keyspaceExists = for {
          _      <- createKeyspace[IO](keySpace)
          result <- createKeyspace[IO](keySpace)
        } yield result

        keyspaceExists.leftValue shouldBe KeyspaceAlreadyExists
      }
    }

    "exception occur" should {

      "return Failed with query syntax error exception" in {

        createKeyspace[IO]("£££%_").leftValue shouldBe CustomError("line 1:16 no viable alternative at character '£'")
      }
    }

    "schema table not exist" should {
      "create schema table" in {

        val frameTableCreated = for {
          _      <- createKeyspace[IO](keySpace)
          result <- createFrameTable[IO](keySpace)
          check  <- checkTable(keySpace, frameTable)
        } yield (result, check)

        frameTableCreated.rightValue shouldBe (FrameTableCreated, true)
      }

    }

    "schema table is empty" should {
      "return None" in {

        val lastScriptApplied = for {
          _                 <- createKeyspace[IO](keySpace)
          _                 <- createFrameTable[IO](keySpace)
          lastScriptApplied <- getLastScriptApplied[IO](keySpace)
        } yield lastScriptApplied

        lastScriptApplied.rightValue shouldBe None
      }

      "return last success applied script" in {

        val lastScriptApplied = for {
          _                 <- createKeyspace[IO](keySpace)
          _                 <- createFrameTable[IO](keySpace)
          _                 <- insertAppliedScript[IO](keySpace, mockAppliedScript(1))
          _                 <- insertAppliedScript[IO](keySpace, mockAppliedScript(2))
          _                 <- insertAppliedScript[IO](keySpace, mockAppliedScript(3, success = false, Some("error")))
          lastScriptApplied <- getLastScriptApplied[IO](keySpace)
        } yield lastScriptApplied

        lastScriptApplied.rightValue.value shouldBe AppliedScript(2,
                                                                  "V2_script.cql",
                                                                  md5("body_2"),
                                                                  LocalDate.now().format(DateTimeFormatter.ISO_DATE),
                                                                  None,
                                                                  success = true,
                                                                  10L)
      }
    }

    "insertTestRecord" should {
      "insert new record in frames table" in {
        val lastScriptApplied = for {
          _                 <- createKeyspace[IO](keySpace)
          _                 <- createFrameTable[IO](keySpace)
          insertResult      <- insertAppliedScript[IO](keySpace, mockAppliedScript(1))
          lastScriptApplied <- getLastScriptApplied[IO](keySpace)
        } yield (insertResult, lastScriptApplied)

        lastScriptApplied.rightValue shouldBe (ScriptApplied, Some(
          AppliedScript(1, "V1_script.cql", md5("body_1"), LocalDate.now().format(DateTimeFormatter.ISO_DATE), None, success = true, 10L)))
      }
    }
  }

  private def checkKeySpace(): EitherT[IO, OperationError, Boolean] =
    EitherT.pure(clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keySpace)).isDefined)).unsafeRunSync())

  private def checkTable(keyspace: String, table: String): EitherT[IO, OperationError, Boolean] =
    EitherT.pure(clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keyspace).getTable(table)).isDefined)).unsafeRunSync())

  private def mockAppliedScript(version: Long, success: Boolean = true, errorMessage: Option[String] = None) =
    AppliedScript(version, s"V${version}_script.cql", md5(s"body_$version"), "2019-01-01", errorMessage, success, 10)
}
