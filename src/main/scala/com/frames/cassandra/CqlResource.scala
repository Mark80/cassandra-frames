package com.frames.cassandra
import scala.io.Source

case class CqlResource(name: String, source: Source) extends AutoCloseable {

  def close(): Unit      = source.close()
  def getContent: String = source.mkString

}

case class CqlFile(name: String, body: String)
