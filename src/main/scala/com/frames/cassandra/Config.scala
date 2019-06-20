package com.frames.cassandra

import com.typesafe.config.ConfigFactory

object Config {
  private val config = ConfigFactory.load()

  lazy val CassandraHost = config.getString("cassandra.host").split(",")
  lazy val CassandraPort = config.getInt("cassandra.port")
  lazy val DefaultScriptFolder = config.getString("cassandra.folder")
}
