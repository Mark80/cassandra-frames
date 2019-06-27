package com.frames.cassandra
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder}
import com.datastax.driver.core.{BoundStatement, Row, Session}

object FramesField {
  val Version       = "version"
  val FileName      = "file_name"
  val Checksum      = "checksum"
  val Date          = "date"
  val Success       = "success"
  val ExecutionTime = "execution_time"
  val ErrorMessage  = "error_message"
}

case class AppliedScript(
    version: Long,
    fileName: String,
    checksum: String,
    date: String,
    errorMessage: Option[String],
    success: Boolean,
    executionTime: Long
)

object FramesOps {

  import FramesField._

  def getAppliedScripts(keyspace: String): String =
    s"SELECT * FROM $keyspace.frames_table WHERE success = true ALLOW FILTERING"

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

  def insertStatement(keyspace: String, withError: Boolean = false): Insert = {
    val insertStatement = QueryBuilder
      .insertInto(keyspace, "frames_table")
      .value(FramesField.Version, QueryBuilder.bindMarker())
      .value(FramesField.FileName, QueryBuilder.bindMarker())
      .value(FramesField.Checksum, QueryBuilder.bindMarker())
      .value(FramesField.Date, QueryBuilder.bindMarker())
      .value(FramesField.Success, QueryBuilder.bindMarker())
      .value(FramesField.ExecutionTime, QueryBuilder.bindMarker())

    if (withError) insertStatement.value(FramesField.ErrorMessage, QueryBuilder.bindMarker())
    insertStatement
  }

  def boundInsertStatement(insertStatement: Insert, appliedScript: AppliedScript)(implicit session: Session): BoundStatement =
    new BoundStatement(session.prepare(insertStatement))
      .setLong(FramesField.Version, appliedScript.version)
      .setString(FramesField.FileName, appliedScript.fileName)
      .setString(FramesField.Checksum, appliedScript.checksum)
      .setString(FramesField.Date, LocalDate.now().format(DateTimeFormatter.ISO_DATE))
      .setBool(FramesField.Success, appliedScript.success)
      .setLong(FramesField.ExecutionTime, appliedScript.executionTime)
      .bind()
}
