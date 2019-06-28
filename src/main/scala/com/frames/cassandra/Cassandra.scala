package com.frames.cassandra

import cats.effect.IO
import com.datastax.driver.core.Cluster

object Cassandra {

  def migrate(cluster: Cluster, keyspace: String): IO[Unit] = IO.unit

}
