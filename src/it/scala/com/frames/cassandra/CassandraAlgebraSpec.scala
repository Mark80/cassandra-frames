package com.frames.cassandra

import cats.effect.IO
import com.datastax.driver.core.KeyspaceMetadata

class CassandraAlgebraSpec extends CassandraBaseSpec {

  override def tables: List[String] = Nil

  override def keySpace: String = "keyspace_name"

  val algebra = CassandraAlgebra

  "KeyspaceDao" when {

    "keyspace not exists" should  {

      "return Created" in {

        val (operationResult, maybeKeyspace) = (for {
          result <- algebra.createKeyspace[IO](keySpace)
          maybeKeyspace <- checkKeySpace()
        } yield (result, maybeKeyspace)).unsafeRunSync()

        operationResult shouldBe OK
        maybeKeyspace should not be empty
      }

    }

    "keyspace exists" should {

      "return Exists" in {

        (for {
          _ <- algebra.createKeyspace[IO](keySpace)
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
  }

  private def checkKeySpace(): IO[Option[KeyspaceMetadata]] =
    clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keySpace))))
}
