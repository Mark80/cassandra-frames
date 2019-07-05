package com.frames.cassandra

import cats.effect.{IO, Resource}
import com.datastax.driver.core.{Cluster, Session}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait CassandraBaseSpec extends Suite with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val clusterResource: Resource[IO, Cluster] =
    Resource.liftF(IO.pure(Cluster.builder().addContactPoints(Config.CassandraHost: _*).withPort(Config.CassandraPort).build()))

  implicit val sessionResource: Resource[IO, Session] =
    clusterResource.map(_.connect())

  def tables: List[String]
  def keySpace: String

  override def beforeEach(): Unit =
    sessionResource
      .use(session => IO(session.execute(s"DROP KEYSPACE IF EXISTS $keySpace")))
      .unsafeRunSync()

  override def afterEach(): Unit =
    cleanTables()

  private def cleanTables(): Unit =
    (for {
      tb <- IO(tables)
      _  <- truncateTables(tb)
    } yield ()).unsafeRunSync()

  private def truncateTables(tables: List[String]): IO[Unit] =
    IO(tables.foreach(table => sessionResource.use(session => IO(session.execute(s"TRUNCATE $keySpace.$table;")))))

  override def afterAll(): Unit =
    (for {
      _ <- sessionResource.use(session => IO(session.execute(s"DROP KEYSPACE IF EXISTS $keySpace")))
      _ <- sessionResource.use(session => IO(session.close()))
      _ <- clusterResource.use(cluster => IO(cluster.close()))
    } yield ()).unsafeRunSync()

}
