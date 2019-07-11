package com.frames.cassandra

import java.io.File

import cats.data.EitherT
import cats.effect.{Resource, Sync}

import scala.io.Source

object ScriptsOps extends ResourceDelay {

  val QueryScriptRegex = "\\s*;\\s*(?=([^']*'[^']*')*[^']*$)"

  def generateSource[F[_]](file: File)(implicit sync: Sync[F]): Resource[F, CqlResource] =
    Resource
      .fromAutoCloseable(sync.delay(CqlResource(file.getName, Source.fromFile(file))))

  def useCqlResource[F[_]](resource: Resource[F, CqlResource])(implicit sync: Sync[F]): ErrorOr[F, CqlFile] =
    useResourceWithDelay { cqlResource: CqlResource =>
      CqlFile(cqlResource.name, cqlResource.getContent)
    }()(sync, resource)

  def loadScripts[F[_]](scriptsFolder: String = Config.DefaultScriptFolder)(implicit sync: Sync[F]): ErrorOr[F, List[CqlFile]] =
    Option(getClass.getResource(scriptsFolder)) match {
      case Some(folder) =>
        getCqlFiles(new File(folder.getPath))
          .foldLeft(ErrorOr.apply(sync.pure(Right(List.empty[CqlFile]): Either[OperationError, List[CqlFile]]))) { (accFiles, file) =>
            for {
              source      <- useCqlResource(generateSource(file))
              resultFiles <- accFiles
            } yield source :: resultFiles
          }
      case None => EitherT.fromEither(Left(ScriptFolderNotExists(scriptsFolder)))
    }

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

  def getScriptWithChangedSource[F[_]](scriptFiles: List[CqlFile], appliedScripts: List[ExecutedScript])(
      implicit sync: Sync[F]
  ): ErrorOr[F, List[ExecutedScript]] =
    withDelay {
      appliedScripts
        .map(applied => toTupleWithFileBody(applied, scriptFiles))
        .filter(hasDifferentChecksum)
        .map(_._1)
    }()

  private def toTupleWithFileBody(appliedScript: ExecutedScript, scriptFiles: List[CqlFile]) =
    (appliedScript, getRelativeScriptFile(appliedScript.fileName, scriptFiles))

  private def getRelativeScriptFile(appliedScriptName: String, scriptFiles: List[CqlFile]) =
    scriptFiles.find(script => script.name == appliedScriptName).map(_.body)

  private def hasDifferentChecksum(tuple: (ExecutedScript, Option[String])) =
    tuple._2.forall(sourceBody => {
      tuple._1.checksum != Checksum.calculate[String](sourceBody)
    })

}
