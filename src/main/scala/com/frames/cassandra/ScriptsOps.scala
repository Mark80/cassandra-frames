package com.frames.cassandra

import java.io.File
import java.security.MessageDigest

import cats.effect.{Resource, Sync}

import scala.io.Source

case class CqlFile(name: String, body: Source)

object ScriptsOps {

  type CqlResource[F[_]]            = Resource[F, List[CqlFile]]
  type ExecutedScriptResource[F[_]] = Resource[F, List[ExecutedScript]]

  def loadScripts[F[_]](scriptsFolder: String = Config.DefaultScriptFolder)(implicit sync: Sync[F]): CqlResource[F] =
    Resource
      .liftF(sync.delay(Option(getClass.getResource(scriptsFolder))))
      .map(mayBeUrl => mayBeUrl.map(url => new File(url.getPath)))
      .map(
        folder =>
          folder
            .map(file => getCqlFiles(file))
            .transpose
            .flatten
            .map(file => CqlFile(file.getName, Source.fromFile(file)))
            .toList
      )

  private def getCqlFiles(folder: File): List[File] =
    folder.listFiles(_.getName.endsWith(".cql")).toList

  def getVariationInScriptResources[F[_]](scriptFiles: List[CqlFile], executedScripts: List[ExecutedScript])(
      implicit sync: Sync[F]
  ): F[List[ExecutedScript]] =
    sync.delay(
      executedScripts
        .map(applied => toTupleWithFileBody(applied, scriptFiles))
        .filter(hasDifferentChecksum)
        .map(_._1)
    )

  private def toTupleWithFileBody(executedScript: ExecutedScript, scriptFiles: List[CqlFile]) =
    (executedScript, getRelativeScriptFile(executedScript.fileName, scriptFiles))

  private def getRelativeScriptFile(executedScriptName: String, scriptFiles: List[CqlFile]) =
    scriptFiles.find(script => script.name == executedScriptName).map(_.body)

  private def hasDifferentChecksum(tuple: (ExecutedScript, Option[Source])) =
    tuple._2.forall(sourceBody => {
      tuple._1.checksum != md5(sourceBody.mkString)
    })

  def md5(s: String): String =
    MessageDigest.getInstance("MD5").digest(s.getBytes).mkString

}
