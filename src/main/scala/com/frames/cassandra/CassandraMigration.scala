package com.frames.cassandra

import cats.effect.IO
import com.datastax.driver.core.Cluster

object CassandraMigration {

  def migrate(cluster: Cluster, keyspace: String): IO[Unit] = IO.unit

}
