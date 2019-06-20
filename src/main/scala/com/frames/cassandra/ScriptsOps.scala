package com.frames.cassandra

import java.io.File
import java.security.MessageDigest

import cats.effect.{Resource, Sync}

import scala.io.Source

object ScriptsOps {

  type CqlFile           = (String, Source)
  type CqlResource[F[_]] = Resource[F, List[CqlFile]]
  type AppliedScript     = (String, String)

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
            .map(file => (file.getName, Source.fromFile(file)))
            .toList
      )

  private def getCqlFiles(folder: File): List[File] =
    folder.listFiles(_.getName.endsWith(".cql")).toList

  def compareAppliedScriptsWithSources[F[_]](scriptFiles: List[CqlFile], appliedScripts: List[AppliedScript])(
      implicit sync: Sync[F]
  ): F[List[AppliedScript]] =
    sync.delay(
      appliedScripts
        .map(applied => (applied, scriptFiles.find(script => script._1 == applied._1).map(_._2)))
        .filter(couple => couple._2.forall(sourceBody => compareChecksum(couple._1._2, sourceBody)))
        .map(_._1)
    )

  private def compareChecksum(applied: String, source: Source) = {
    val appliedText = applied
    val scriptText  = source.mkString
    appliedText != md5(scriptText)
  }

  def md5(s: String): String =
    MessageDigest.getInstance("MD5").digest(s.getBytes).mkString

}
