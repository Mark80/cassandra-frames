package com.frames.cassandra

object KeyspaceOps {

  def create(keyspace: String): String =
    s"""CREATE KEYSPACE $keyspace
       | WITH replication = {
       | 'class':'$replicationClass',
       | 'replication_factor' : $replicationFactor
           };""".stripMargin

  val replicationClass = "SimpleStrategy"
  val replicationFactor = 1

}
