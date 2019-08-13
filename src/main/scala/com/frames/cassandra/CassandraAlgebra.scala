package com.frames.cassandra

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import cats.effect._
import com.datastax.driver.core.{Cluster, Session}
import com.datastax.driver.core.exceptions.AlreadyExistsException

import scala.collection.JavaConverters._

object CassandraAlgebra extends ResourceDelay {

  def createFrameTable[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    useResourceWithDelay[F, Session, OperationResult] { session =>
      session.execute(InitializationOps.createFrameTable(keySpace))
      FrameTableCreated
    } {
      case _: AlreadyExistsException => FrameTableAlreadyExists
    }

  def createKeyspace[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    useResourceWithDelay[F, Session, OperationResult] { session =>
      if (keyspaceNotExist(keySpace, session)) {
        session
          .execute(InitializationOps.createKeyspace(keySpace))
        KeyspaceCreated
      } else
        KeyspaceCreated
    } {
      case _: AlreadyExistsException => KeyspaceAlreadyExists
    }

  private def keyspaceNotExist[F[_]](keySpace: String, session: Session) =
    Option(session.getCluster.getMetadata.getKeyspace(keySpace)).isEmpty

  def getExecutedScripts[F[_]](keySpace: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, List[ExecutedScript]] =
    useResourceWithDelay[F, Session, List[ExecutedScript]] { session =>
      session
        .execute(FramesOps.getExecutedStatement(keySpace))
        .iterator()
        .asScala
        .toList
        .map(FramesOps.toExecutedScript)
    }()

  def getLastSuccessfulExecutedScript[F[_]](
      keySpace: String
  )(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, Option[ExecutedScript]] =
    useResourceWithDelay[F, Session, Option[ExecutedScript]] { session =>
      session
        .execute(FramesOps.getSuccessfulExecutedStatement(keySpace))
        .iterator()
        .asScala
        .toList
        .headOption
        .map(FramesOps.toExecutedScript)
    }()

  def executeAllScript(
      scripts: List[CqlFile],
      keyspace: String
  )(implicit sessionResource: Resource[IO, Session], clock: java.time.Clock, checksumAlgorithm: ChecksumAlgorithm[String]): ErrorOr[IO, Unit] =
    scripts.foldLeft(ErrorOr.liftF[IO, Unit](IO(Unit))) { (acc, cqlFile) =>
      for {
        _        <- executeQuery[IO](cqlFile.body)
        checkSum <- ErrorOr.liftF(IO(Checksum.calculate(cqlFile.body)))
        executedScript <- ErrorOr.liftF(
          IO(ExecutedScript.from(cqlFile, checkSum, LocalDate.now(clock).format(DateTimeFormatter.ISO_DATE), None, 100L))
        )
        _ <- insertExecutedScript[IO](keyspace, executedScript)
        _ <- acc
      } yield ()

    }

  def executeQuery[F[_]](query: String)(implicit sync: Sync[F], sessionResource: Resource[F, Session]): ErrorOr[F, OperationResult] =
    useResourceWithDelay[F, Session, OperationResult] { session =>
      session.execute(query)
      QueryExecuted
    }()

  def insertExecutedScript[F[_]](
      keySpace: String,
      executedScript: ExecutedScript
  )(implicit sync: Sync[F], sessionResource: Resource[F, Session], clock: java.time.Clock): ErrorOr[F, OperationResult] =
    useResourceWithDelay[F, Session, OperationResult] { session =>
      session.execute(FramesOps.boundInsertStatement(FramesOps.insertStatement(keySpace, !executedScript.success), executedScript)(session, clock))
      FrameCreated
    }()
}
