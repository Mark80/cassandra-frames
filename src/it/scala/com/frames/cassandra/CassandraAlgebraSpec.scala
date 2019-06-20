package com.frames.cassandra

import cats.effect.IO
import com.datastax.driver.core.KeyspaceMetadata

class CassandraAlgebraSpec extends CassandraBaseSpec {

  val frameTable = "frames_table"

  override def tables: List[String] = List(frameTable)

  override def keySpace: String = "keyspace_name"

  val algebra = CassandraAlgebra

  "KeyspaceDao" when {

    "keyspace not exists" should {

      "return Created" in {

        val (operationResult, keySpaceCreated) = (for {
          result        <- algebra.createKeyspace[IO](keySpace)
          maybeKeyspace <- checkKeySpace()
        } yield (result, maybeKeyspace)).unsafeRunSync()

        operationResult shouldBe OK
        keySpaceCreated shouldBe true
      }

    }

    "keyspace exists" should {

      "return Exists" in {

        (for {
          _      <- algebra.createKeyspace[IO](keySpace)
          result <- algebra.createKeyspace[IO](keySpace)
        } yield result)
          .unsafeRunSync() shouldBe KeyspaceAlreadyExists
      }
    }

    "exception occur" should {

      "return Failed with query syntax error message" in {

        algebra.createKeyspace[IO]("£££%_").unsafeRunSync() shouldBe Error("line 1:16 no viable alternative at character '£'")
      }
    }

    "schema table not exist" should {
      "create schema table" in {

        val (createResult, check) = (for {
          _      <- algebra.createKeyspace[IO](keySpace)
          result <- algebra.createFrameTable[IO](keySpace)
          check  <- checkTable(keySpace, frameTable)
        } yield (result, check))
          .unsafeRunSync()

        createResult shouldBe FrameTableCreated
        check shouldBe true

      }

    }
  }

  private def checkKeySpace(): IO[Boolean] =
    clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keySpace)).isDefined))

  private def checkTable(keyspace: String, table: String): IO[Boolean] =
    clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keyspace).getTable(table)).isDefined))
}
