package org.covid82.registryloader

import cats.effect.{Async, ContextShift}
import fs2.{Pipe, Stream}
import ray.fs2.ftp.Ftp
import ray.fs2.ftp.Ftp.connect
import ray.fs2.ftp.settings.FtpCredentials.credentials
import ray.fs2.ftp.settings.FtpSettings

import scala.concurrent.{ExecutionContextExecutor => ECE}

case class FtpConfig(
  path: String,
  host: String,
  port: Int,
  user: String,
  pass: String
)

class FtpRegistryReader[F[_]](config: FtpConfig) extends RegistryReader[F] {

  private def dropEmptyLinesAndComments: Pipe[F, String, String] = _.filter(_.trim.nonEmpty).filter(!_.startsWith("#"))

  private def dropHeader: Pipe[F, String, String] = _.drop(1)

  private def splitAndDropSummary: Pipe[F, String, List[String]] = _
    .map(s => s.split('|').toList)
    .filter(s => s(1) != "*")

  private def createRows: Pipe[F, List[String], RipeRecord] = _.collect {
    case registry :: cc :: typ :: start :: value :: date :: status :: extensions =>
      RipeRecord(registry, cc, typ, start, value, date, status, extensions.fold("")(_ + "|" + _))
  }

  protected def read(implicit ec: ECE, cs: ContextShift[F], as: Async[F]): Stream[F, RipeRecord] = {
    val credential = credentials(config.user, config.pass)
    val settings = FtpSettings(config.host, config.port, credential)
    for {
      client <- Stream.resource(connect[F](settings))
      stream <- Ftp.readFile[F](config.path)(client)
        .through(fs2.text.utf8Decode)
        .through(fs2.text.lines)
        .through(dropEmptyLinesAndComments)
        .through(dropHeader)
        .through(splitAndDropSummary)
        .through(createRows)
    } yield stream
  }
}