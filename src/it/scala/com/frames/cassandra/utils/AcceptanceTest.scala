package com.frames.cassandra.utils

import cats.effect.{IO, Resource}
import com.datastax.driver.core.{Cluster, Session}
import com.frames.cassandra.{Cassandra, CassandraBaseSpec}
import org.scalatest._

class AcceptanceTest extends FeatureSpec with GivenWhenThen with Matchers with OptionValues with CassandraBaseSpec {

  val tables     = List("table1", "table2", "table3")
  val FrameTable = "frame_table"
  val keySpace   = "test"

  feature("Execute all script") {

    scenario("migrate is invoked on empty cassandra schema") {

      Given("an empty cassandra schema")

      When("migrate is invoked")
      executeMigrations()

      Then("frame table is created and contains executed script")
      checkExistingTable(FrameTable)
      checkSizeOfFrameTable(3)

      Then("schemas are created")
      checkScriptsTableHasBeCreated()

    }

  }

  private def checkSizeOfFrameTable(expectedSize: Long)(implicit sessionResource: Resource[IO, Session]): Unit =
    sessionResource
      .use(session =>
        for {
          result <- IO(session.execute("SELECT COUNT(*) from frame_table").one())
          _      <- IO(result.getLong(0) shouldBe expectedSize)
        } yield ())
      .unsafeRunSync()

  private def checkScriptsTableHasBeCreated()(implicit clusterResource: Resource[IO, Cluster]): Unit =
    tables.foreach(table => checkExistingTable(table))

  private def executeMigrations()(implicit clusterResource: Resource[IO, Cluster]): Unit =
    clusterResource.use { cluster =>
      Cassandra.migrate(cluster, keySpace)
    }.unsafeRunSync()

  private def checkExistingTable(table: String)(implicit clusterResource: Resource[IO, Cluster]): Assertion =
    clusterResource.use { cluster =>
      IO(Option(cluster.getMetadata.getKeyspace(keySpace).getTable(table)))
    }.unsafeRunSync() should not be empty

}
