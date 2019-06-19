package com.frames.cassandra

import java.io.File

import cats.effect.Sync

import scala.io.Source

object ScriptLoader {

  def loadScripts[F[_]](scriptsFolder: String = "/migration/scripts")(implicit sync: Sync[F]): F[List[Source]] = sync.delay {
    val maybeFolder: Option[File] = for {
      path <- Option(getClass.getResource(scriptsFolder))
      folder <- Option(new File(path.getPath))
    } yield folder

    maybeFolder match {
      case Some(f) =>
        f.listFiles(_.getName.endsWith(".cql"))
          .map(Source.fromFile)
          .toList
      case None =>
        Nil
    }
  }
}
