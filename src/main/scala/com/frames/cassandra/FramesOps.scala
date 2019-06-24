package com.frames.cassandra
import com.datastax.driver.core.Row

object FramesField {
  val Version       = "version"
  val FileName      = "file_name"
  val Checksum      = "checksum"
  val Date          = "date"
  val Success       = "success"
  val ExecutionTime = "execution_time"
  val ErrorMessage  = "error_message"
}

object FramesOps {

  import FramesField._

  def getLastAppliedScript[F[_]](keyspace: String): String =
    s"SELECT * FROM $keyspace.frames_table WHERE success = true LIMIT 1 ALLOW FILTERING"
  //QueryBuilder.select().from(keyspace).where(QueryBuilder.eq("success", true)).limit(1)

  def toAppliedScript(row: Row): AppliedScript =
    AppliedScript(
      row.getLong(Version),
      row.getString(FileName),
      row.getString(Checksum),
      row.getString(Date),
      Option(row.getString(ErrorMessage)),
      row.getBool(Success),
      row.getLong(ExecutionTime)
    )
}
