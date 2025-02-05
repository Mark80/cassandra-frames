package com.frames.cassandra

import java.time.{Clock, LocalDate}
import java.time.format.DateTimeFormatter

import cats.data.EitherT
import cats.effect.IO
import com.frames.cassandra.utils.EitherTValues
import org.scalatest.{Matchers, OptionValues, WordSpec}

class CassandraAlgebraSpec extends WordSpec with Matchers with CassandraBaseSpec with OptionValues with EitherTValues with FixedClock {

  val frameTable = "frames_table"

  override def tables: List[String] = List(frameTable)

  override def keySpace: String = "keyspace_name"

  import CassandraAlgebra._

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

        val lastScriptExecutedWithSuccess = for {
          _                             <- createKeyspace[IO](keySpace)
          _                             <- createFrameTable[IO](keySpace)
          lastScriptExecutedWithSuccess <- getLastSuccessfulExecutedScript[IO](keySpace)
        } yield lastScriptExecutedWithSuccess

        lastScriptExecutedWithSuccess.rightValue shouldBe None
      }

      "return last success applied script" in {

        val lastScriptExecutedWithSuccess: EitherT[IO, OperationError, Option[ExecutedScript]] = for {
          _                             <- createKeyspace[IO](keySpace)
          _                             <- createFrameTable[IO](keySpace)
          _                             <- insertExecutedScript[IO](keySpace, mockExecutedScript(1))
          _                             <- insertExecutedScript[IO](keySpace, mockExecutedScript(2))
          _                             <- insertExecutedScript[IO](keySpace, mockExecutedScript(3, success = false, Some("error")))
          lastScriptExecutedWithSuccess <- getLastSuccessfulExecutedScript[IO](keySpace)
        } yield lastScriptExecutedWithSuccess

        lastScriptExecutedWithSuccess.rightValue.value shouldBe ExecutedScript(
          2,
          "V2_script.cql",
          Checksum.calculate[String]("body_2"),
          LocalDate.now(fixedClock).format(DateTimeFormatter.ISO_DATE),
          None,
          success = true,
          10L
        )
      }
    }

    "insertTestRecord" should {
      "insert new record in frames table" in {
        val lastScriptExecutedWithSuccess = for {
          _                             <- createKeyspace[IO](keySpace)
          _                             <- createFrameTable[IO](keySpace)
          insertResult                  <- insertExecutedScript[IO](keySpace, mockExecutedScript(1))
          lastScriptExecutedWithSuccess <- getLastSuccessfulExecutedScript[IO](keySpace)
        } yield (insertResult, lastScriptExecutedWithSuccess)

        lastScriptExecutedWithSuccess.rightValue shouldBe (FrameCreated, Some(
          ExecutedScript(1,
                         "V1_script.cql",
                         Checksum.calculate[String]("body_1"),
                         LocalDate.now(fixedClock).format(DateTimeFormatter.ISO_DATE),
                         None,
                         success = true,
                         10L)))
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

    "retrieving executed script" should {

      "return the list of executed script" in {

        val script1 = mockExecutedScript(1)
        val script2 = mockExecutedScript(2)
        val script3 = mockExecutedScript(3, success = false, Some("error"))

        val allScripts = for {
          _          <- createKeyspace[IO](keySpace)
          _          <- createFrameTable[IO](keySpace)
          _          <- insertExecutedScript[IO](keySpace, script1)
          _          <- insertExecutedScript[IO](keySpace, script2)
          _          <- insertExecutedScript[IO](keySpace, script3)
          allScripts <- getExecutedScripts[IO](keySpace)
        } yield allScripts

        val expectedScripts = List(script1, script2, script3)

        allScripts.rightValue should contain theSameElementsAs expectedScripts

      }

      "return an empty list if no script are applied" in {

        val allScripts = for {
          _          <- createKeyspace[IO](keySpace)
          _          <- createFrameTable[IO](keySpace)
          allScripts <- getExecutedScripts[IO](keySpace)
        } yield allScripts

        allScripts.rightValue shouldBe empty
      }

      "return an Error if some problem prevent to get the result" in {

        val executedScripts = getExecutedScripts[IO](keySpace)

        executedScripts.leftValue shouldBe a[OperationError]
      }
    }
  }

  private def checkKeySpace(): EitherT[IO, OperationError, Boolean] =
    EitherT.pure(clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keySpace)).isDefined)).unsafeRunSync())

  private def checkTable(keyspace: String, table: String): EitherT[IO, OperationError, Boolean] =
    EitherT.pure(clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keyspace).getTable(table)).isDefined)).unsafeRunSync())

  private def mockExecutedScript(version: Long, success: Boolean = true, errorMessage: Option[String] = None)(implicit clock: Clock) =
    ExecutedScript(version,
                   s"V${version}_script.cql",
                   Checksum.calculate[String](s"body_$version"),
                   LocalDate.now(clock).toString,
                   errorMessage,
                   success,
                   10)
}
