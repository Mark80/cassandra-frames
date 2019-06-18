package com.frames.cassandra.dao

import cats.effect.{IO, Resource}
import com.datastax.driver.core.{Cluster, Session}
import com.datastax.driver.core.exceptions.{AlreadyExistsException, QueryValidationException}

sealed trait KeyspaceCreationResult
object Created extends KeyspaceCreationResult
object Exists extends KeyspaceCreationResult
case class Failed(msg: String) extends KeyspaceCreationResult

trait KeyspaceDaoTrait[F[_]] {

  def createKeyspace(keyspace: String)(implicit cluster: Resource[IO, Cluster]): F[KeyspaceCreationResult]
}

class KeyspaceDao(implicit sessionResource: Resource[IO, Session]) extends KeyspaceDaoTrait[IO] {

  override def createKeyspace(keyspace: String)(implicit clusterResource: Resource[IO, Cluster]): IO[KeyspaceCreationResult] =
    for {
      maybeKeyspace <- clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keyspace))))
      result <- maybeKeyspace match {
        case Some(_) => IO(Exists)
        case None =>
          sessionResource.use(
            session =>
              IO {
                try {
                  session.execute(Keyspace.create(keyspace))
                  Created
                } catch {
                  case _: AlreadyExistsException   => Exists
                  case e: QueryValidationException => Failed(s"Query syntax problem: ${e.getLocalizedMessage}")
                  case e: RuntimeException         => Failed(s"Unexpected exception: ${e.getMessage}")
                }
              }
          )
      }
    } yield result
}

object Keyspace {

  def create(keyspace: String): String =
    s"""CREATE KEYSPACE $keyspace
       | WITH replication = {
       | 'class':'$replicationClass',
       | 'replication_factor' : $replicationFactor
           };""".stripMargin

  val replicationClass = "SimpleStrategy"
  val replicationFactor = 1
}
