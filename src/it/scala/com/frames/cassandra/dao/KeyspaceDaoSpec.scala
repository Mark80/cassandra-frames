package com.frames.cassandra.dao

import com.frames.cassandra.CassandraBaseSpec

class KeyspaceDaoSpec extends CassandraBaseSpec {

  override def tables: List[String] = Nil

  override def keySpace: String = "keyspace_name"

  val dao = new KeyspaceDao

  "KeyspaceDao" when {

    "keyspace not exists" should  {

      "return Created" in {

        dao.createKeyspace(keySpace).unsafeRunSync() shouldBe Created
      }

    }

    "keyspace exists" should {

      "return Exists" in {

        dao.createKeyspace(keySpace).unsafeRunSync() shouldBe Exists
      }
    }

    "exception occur" should {

      "return Failed with query syntax error message" in {

        dao.createKeyspace("£££%_").unsafeRunSync() shouldBe Failed("Query syntax problem: line 1:16 no viable alternative at character '£'")
      }
    }
  }
}
