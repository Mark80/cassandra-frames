package com.frames.cassandra

import java.io.File
import java.security.MessageDigest

import cats.effect.Sync

import scala.io.Source

case class CqlFile(name: String, body: Source)

object ScriptsOps extends ResourceDelay {

  val QueryScriptRegex = "\\s*;\\s*(?=([^']*'[^']*')*[^']*$)"

  def loadScripts[F[_]](scriptsFolder: String = Config.DefaultScriptFolder)(implicit sync: Sync[F]): ErrorOr[F, List[CqlFile]] =
    withDelay {
      Option(getClass.getResource(scriptsFolder))
        .map(url => new File(url.getPath))
        .map(file => getCqlFiles(file))
        .transpose
        .flatten
        .map(file => CqlFile(file.getName, Source.fromFile(file)))
        .toList
    }

  def splitScriptSource[F[_]](files: List[CqlFile])(implicit sync: Sync[F]): ErrorOr[F, Map[String, List[String]]] =
    withDelay {
      files
        .groupBy(file => file.name)
        .mapValues { files =>
          for {
            file      <- files
            statement <- file.body.mkString.split(QueryScriptRegex).toList
          } yield statement
        }
    }

  private def getCqlFiles(folder: File): List[File] =
    folder.listFiles(_.getName.endsWith(".cql")).toList

  def getScriptWithChangedSource[F[_]](scriptFiles: List[CqlFile], appliedScripts: List[AppliedScript])(
      implicit sync: Sync[F]
  ): ErrorOr[F, List[AppliedScript]] =
    withDelay {
      appliedScripts
        .map(applied => toTupleWithFileBody(applied, scriptFiles))
        .filter(hasDifferentChecksum)
        .map(_._1)
    }

  private def toTupleWithFileBody(appliedScript: AppliedScript, scriptFiles: List[CqlFile]) =
    (appliedScript, getRelativeScriptFile(appliedScript.fileName, scriptFiles))

  private def getRelativeScriptFile(appliedScriptName: String, scriptFiles: List[CqlFile]) =
    scriptFiles.find(script => script.name == appliedScriptName).map(_.body)

  private def hasDifferentChecksum(tuple: (AppliedScript, Option[Source])) =
    tuple._2.forall(sourceBody => {
      tuple._1.checksum != md5(sourceBody.mkString)
    })

  def md5(s: String): String =
    MessageDigest.getInstance("MD5").digest(s.getBytes).mkString

}
