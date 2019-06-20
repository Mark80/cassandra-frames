package com.frames.cassandra

import java.io.File

import cats.effect.{Resource, Sync}

import scala.io.Source

object ScriptLoader {

  type CqlFile = Source
  type CqlResource[F[_]] = Resource[F, List[CqlFile]]

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
            .map(Source.fromFile)
            .toList
      )

  private def getCqlFiles(folder: File): List[File] =
    folder.listFiles(_.getName.endsWith(".cql")).toList
}
