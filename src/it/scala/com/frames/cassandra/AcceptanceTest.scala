package com.frames.cassandra

import cats.effect.{IO, Resource}
import com.datastax.driver.core.{Cluster, Session}
import org.scalatest._

import scala.collection.JavaConverters._

class AcceptanceTest extends fixture.FeatureSpec with GivenWhenThen with Matchers with OptionValues with CassandraBaseSpec with FixedClock {

  implicit val testAlgorithm: ChecksumAlgorithm[String] = (input: String) => "checksum"

  val tables     = List("table1", "table2", "table3")
  val FrameTable = "frames_table"
  val keySpace   = "test"

  feature("Execute all script") {

    scenario("migrate is invoked on empty cassandra schema") { expectedScripts =>
      Given("an empty cassandra schema")

      When("migrate is invoked")
      executeMigrations()

      Then("frame table is created and contains executed script")
      checkExistingTable(FrameTable)
      checkScriptsContents(expectedScripts)

      Then("schemas are created")
      checkScriptsTableHasBeCreated()

    }

  }

  private def checkScriptsContents(expectedScript: List[ExecutedScript]): List[ExecutedScript] =
    sessionResource
      .use(session =>
        for {
          rows    <- IO(selectAllRecodInFrameTable(session))
          scripts <- IO(rows.map(FramesOps.toExecutedScript))
          _       <- IO(assertContent(scripts, expectedScript))
        } yield scripts)
      .unsafeRunSync()

  private def selectAllRecodInFrameTable(session: Session) =
    session.execute("select * from test.frames_table").all().asScala.toList

  private def assertContent(scripts: List[ExecutedScript], expectedResult: List[ExecutedScript]): Unit =
    scripts should contain theSameElementsAs expectedResult

  private def checkScriptsTableHasBeCreated()(implicit clusterResource: Resource[IO, Cluster]): Unit =
    tables.foreach(table => checkExistingTable(table))

  private def executeMigrations()(implicit clusterResource: Resource[IO, Cluster]): Unit =
    clusterResource.use { cluster =>
      Cassandra.migrate(cluster, keySpace)(fixedClock, testAlgorithm)
    }.unsafeRunSync()

  private def checkExistingTable(table: String)(implicit clusterResource: Resource[IO, Cluster]): Assertion =
    clusterResource.use { cluster =>
      IO({
        val metadata = cluster.getMetadata.getKeyspace(keySpace)
        Option(metadata.getTable(table))
      })
    }.unsafeRunSync() should not be empty

  protected def withFixture(test: OneArgTest): Outcome = {

    val expectedScript = List(
      ExecutedScript(
        1L,
        "V1_script1.cql",
        "checksum",
        "2019-01-01",
        None,
        true,
        100L
      ),
      ExecutedScript(
        2L,
        "V2_script2.cql",
        "checksum",
        "2019-01-01",
        None,
        true,
        100L
      ),
      ExecutedScript(
        3L,
        "V3_script3.cql",
        "checksum",
        "2019-01-01",
        None,
        true,
        100L
      )
    )

    withFixture(test.toNoArgTest(expectedScript))

  }

  type FixtureParam = List[ExecutedScript]
}
