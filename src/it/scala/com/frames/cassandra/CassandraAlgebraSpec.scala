package com.frames.cassandra

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import cats.data.EitherT
import cats.effect.IO
import com.frames.cassandra.utils.EitherTValues
import org.scalatest.{Matchers, OptionValues}

class CassandraAlgebraSpec extends CassandraBaseSpec with OptionValues with EitherTValues with Matchers {

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

        val lastScriptApplied: EitherT[IO, OperationError, Option[AppliedScript]] = for {
          _                 <- createKeyspace[IO](keySpace)
          _                 <- createFrameTable[IO](keySpace)
          _                 <- insertAppliedScript[IO](keySpace, mockAppliedScript(1))
          _                 <- insertAppliedScript[IO](keySpace, mockAppliedScript(2))
          _                 <- insertAppliedScript[IO](keySpace, mockAppliedScript(3, success = false, Some("error")))
          lastScriptApplied <- getLastScriptApplied[IO](keySpace)
        } yield lastScriptApplied

        lastScriptApplied.rightValue.value shouldBe AppliedScript(
          2,
          "V2_script.cql",
          md5("body_2"),
          LocalDate.now().format(DateTimeFormatter.ISO_DATE),
          None,
          success = true,
          10L
        )
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

        lastScriptApplied.rightValue shouldBe (FrameCreated, Some(
          AppliedScript(1, "V1_script.cql", md5("body_1"), LocalDate.now().format(DateTimeFormatter.ISO_DATE), None, success = true, 10L)
        ))
      }
    }

    "executeScript" should {
      "execute single query" in {
        val executionResult = for {
          _             <- createKeyspace[IO](keySpace)
          _             <- createFrameTable[IO](keySpace)
          executeResult <- executeQuery[IO](s"CREATE TABLE $keySpace.test(a text PRIMARY KEY)")
          verifyExecute <- executeQuery[IO](s"SELECT * FROM $keySpace.test")
        } yield (executeResult, verifyExecute)

        executionResult.rightValue shouldBe (QueryExecuted, QueryExecuted)
      }

      "return a custom error if query has syntax error" in {
        val executionResult = for {
          _             <- createKeyspace[IO](keySpace)
          _             <- createFrameTable[IO](keySpace)
          executeResult <- executeQuery[IO](s"CREATE TABE $keySpace.test(a text);")
        } yield executeResult

        executionResult.leftValue shouldBe a[CustomError]
      }
    }

    "retrieving applied script" should {

      "return the list of applied script" in {

        val allScripts = for {
          _          <- createKeyspace[IO](keySpace)
          _          <- createFrameTable[IO](keySpace)
          _          <- insertAppliedScript[IO](keySpace, mockAppliedScript(1))
          _          <- insertAppliedScript[IO](keySpace, mockAppliedScript(2))
          _          <- insertAppliedScript[IO](keySpace, mockAppliedScript(3, success = false, Some("error")))
          allScripts <- getAllScripts[IO](keySpace)
        } yield allScripts

        val expectedScripts = List(
          AppliedScript(
            1,
            "V1_script.cql",
            md5("body_1"),
            LocalDate.now().format(DateTimeFormatter.ISO_DATE),
            None,
            success = true,
            10L
          ),
          AppliedScript(
            2,
            "V2_script.cql",
            md5("body_2"),
            LocalDate.now().format(DateTimeFormatter.ISO_DATE),
            None,
            success = true,
            10L
          ),
          AppliedScript(
            3,
            "V3_script.cql",
            md5("body_3"),
            LocalDate.now().format(DateTimeFormatter.ISO_DATE),
            None,
            success = false,
            10L
          )
        )

        allScripts.rightValue should contain theSameElementsAs expectedScripts

      }

      "return an empty list if no script are applied" in {

        val allScripts = for {
          _          <- createKeyspace[IO](keySpace)
          _          <- createFrameTable[IO](keySpace)
          allScripts <- getAllScripts[IO](keySpace)
        } yield allScripts

        allScripts.rightValue shouldBe empty
      }

      "return an Error if some problem prevent to get the result" in {

        val appliedScripts = getAllScripts[IO](keySpace)

        appliedScripts.leftValue shouldBe a[OperationError]
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
