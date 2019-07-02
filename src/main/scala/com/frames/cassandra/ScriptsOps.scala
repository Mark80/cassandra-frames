package com.frames.cassandra

import java.io.File

import cats.effect.{Resource, Sync}

import scala.io.Source

case class CqlFile(name: String, body: String)

object ScriptsOps extends ResourceDelay {

  val QueryScriptRegex = "\\s*;\\s*(?=([^']*'[^']*')*[^']*$)"

  def generateSource[F[_]](file: File)(implicit sync: Sync[F]): Resource[F, Source] =
    Resource
      .fromAutoCloseable(sync.delay(Source.fromFile(file)))

  def loadScripts[F[_]](scriptsFolder: String = Config.DefaultScriptFolder)(implicit sync: Sync[F]): ErrorOr[F, List[Resource[F, Source]]] =
    withDelay {
      Option(getClass.getResource(scriptsFolder))
        .map(url => new File(url.getPath))
        .map(folder => getCqlFiles(folder))
        .transpose
        .flatten
        .toList
        .map(generateSource)
    } {
      case _: Throwable => ScriptsLoadingFailed
    } //.map(files => files.map(file => generateSource(file).use(source => sync.suspend(CqlFile(file.getName, source.mkString)))))

  def splitScriptSource[F[_]](files: List[CqlFile])(implicit sync: Sync[F]): ErrorOr[F, Map[String, List[String]]] =
    withDelay {
      files
        .groupBy(file => file.name)
        .mapValues { files =>
          for {
            file      <- files
            statement <- file.body.split(QueryScriptRegex).toList
          } yield statement
        }
    }()

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
    }()

  private def toTupleWithFileBody(appliedScript: AppliedScript, scriptFiles: List[CqlFile]) =
    (appliedScript, getRelativeScriptFile(appliedScript.fileName, scriptFiles))

  private def getRelativeScriptFile(appliedScriptName: String, scriptFiles: List[CqlFile]) =
    scriptFiles.find(script => script.name == appliedScriptName).map(_.body)

  private def hasDifferentChecksum(tuple: (AppliedScript, Option[String])) =
    tuple._2.forall(sourceBody => {
      tuple._1.checksum != FramesOps.md5(sourceBody)
    })

}
