package com.frames.cassandra

object InitializationOps {

  def create(keyspace: String): String =
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
      |    PRIMARY KEY (version),
      |    version text,
      |    checksum bigint,
      |    date text,
      |    error_message text,
      |    success boolean,
      |    execution_time bigint
      |);
    """.stripMargin

}
