package com.frames.cassandra

import cats.effect.IO
import com.datastax.driver.core.KeyspaceMetadata

class KeyspaceSpec extends CassandraBaseSpec {
  val tables: List[String] = Nil
  val keySpace: String = "key_space"

  "Test" should {
    "create a work space" in {

      val existingKeyspace = (for {
        _ <- createKeyspace()
        exist <- checkKeySpace()
      } yield exist).unsafeRunSync()

      existingKeyspace should not be empty

    }

  }

  private def checkKeySpace(): IO[Option[KeyspaceMetadata]] =
    clusterResource.use(cluster => IO(Option(cluster.getMetadata.getKeyspace(keySpace))))

  private def createKeyspace(): IO[Unit] =
    sessionResource.use(session => IO(session.execute("""CREATE KEYSPACE key_space
        WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};"""))).map(_ => ())
}
