package com.frames.cassandra

object InitializationOps {

  def createKeyspace(keyspace: String): String =
    s"""CREATE KEYSPACE $keyspace
       | WITH replication = {
       | 'class':'$replicationClass',
       | 'replication_factor' : $replicationFactor
           };""".stripMargin

  val replicationClass  = "SimpleStrategy"
  val replicationFactor = 1

  def createFrameTable(keyspace: String): String =
    s"""
      |CREATE TABLE IF NOT EXISTS $keyspace.frames_table (
      |    version bigint,
      |    file_name text,
      |    checksum text,
      |    date text,
      |    error_message text,
      |    success boolean,
      |    execution_time bigint,
      |    PRIMARY KEY (version)
      |);
    """.stripMargin

}
