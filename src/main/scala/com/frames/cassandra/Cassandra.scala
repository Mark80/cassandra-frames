package com.frames.cassandra

import cats.effect.{IO, Resource}
import com.datastax.driver.core.{Cluster, Session}
import com.frames.cassandra.CassandraAlgebra._
import com.frames.cassandra.ScriptsOps._

object Cassandra {

  def migrate(
      cluster: Cluster,
      keyspace: String
  )(implicit clock: java.time.Clock, checksumAlgorithm: ChecksumAlgorithm[String]): IO[Either[OperationError, Unit]] = {

    implicit val connectionResource: Resource[IO, Cluster] = Resource.liftF(IO.pure(cluster))
    implicit val sessionResource: Resource[IO, Session]    = connectionResource.map(_.connect())

    (for {
      _       <- createKeyspace[IO](keyspace)
      _       <- createFrameTable[IO](keyspace)
      scripts <- loadScripts[IO]()
      _ = println(scripts)
      _ <- executeAllScript(scripts, keyspace)
    } yield ()).value

  }

}
