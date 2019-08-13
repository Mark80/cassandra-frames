import java.time.Clock

import com.datastax.driver.core.Cluster
import com.frames.cassandra.{Cassandra, Config, OperationError}

object MainTest {

  def main(args: Array[String]): Unit = {

    implicit val clock = Clock.systemDefaultZone()
    val cluster        = Cluster.builder().addContactPoints(Config.CassandraHost: _*).withPort(Config.CassandraPort).build()

    println(cluster.getMetadata)
    Cassandra.migrate(cluster, "test").unsafeRunSync() match {
      case Right(_)                 => ()
      case Left(ex: OperationError) => println(ex)
    }
    cluster.close()

  }

}
