package org.covid82.registryloader

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.kernel.Monoid
import org.covid82.registryloader.RegistryWriter.write
import org.http4s.server.blaze.BlazeServerBuilder
import fs2.Stream._
import cats.kernel.instances.int.catsKernelStdGroupForInt

object Main extends IOApp {

  val ftpConfig: FtpConfig = FtpConfig(
    path = "/pub/stats/ripencc/delegated-ripencc-latest",
    host = "ftp.ripe.net", port = 21,
    user = "anonymous", pass = ""
  )

  import scala.concurrent.duration._
  import fs2.Stream
  import cats.effect.{ConcurrentEffect, Timer, Async, ContextShift}
  import cats.syntax.apply._

  def server[F[_] : ConcurrentEffect : Timer]: Stream[F, Nothing] = BlazeServerBuilder[F]
    .bindHttp(host = "0.0.0.0", port = 8080)
    .withHttpApp(monitoringRoute[F].orNotFound)
    .serve.drain

  def log[F[_] : Sync](msg: String): Stream[F, Unit] = eval[F, Unit](Sync[F].delay(println(msg)))

  def count[F[_], A](fa: Stream[F, A]): Stream[F, Int] = fa.foldMap(_ => 1)

  def sum[F[_], A: Monoid](s: Stream[F, A]): Stream[F, A] = s.foldMonoid

  def read[F[_] : Timer : Async : ContextShift](
    service: String,
    user: String,
    pass: String
  ): Stream[F, Unit] = {
    val reader = RegistryReader.ftp[F](ftpConfig)
    log("started reading registry") *> {
      val rows = reader.readRows
      count(rows).flatMap(size => log(s"loaded $size records")) *> {
        sum(rows.through(write(service, user, pass))).flatMap { count =>
          log(s"saved $count records into database")
        }
      }.drain
    }
  }

  override def run(args: List[String]): IO[ExitCode] = for {
    (service, user, pass, delay) <- IO.delay {
      val service = sys.env.getOrElse("DB_SERVICE", "localhost:5432")
      val user = sys.env.getOrElse("DB_USER", "postgres")
      val pass = sys.env.getOrElse("DB_PASS", "postgres")
      val delay = sys.env.getOrElse("LOAD_FREQUENCY", "5minutes")
      (service, user, pass, delay)
    }
    _ <- {
      import scala.util.Try
      val readAndWrite = read[IO](service, user, pass)
      val duration = Try(Duration(delay))
        .map(duration => FiniteDuration(duration.length, duration.unit))
        .getOrElse(1.day)
      val stream = readAndWrite ++ Stream.awakeEvery[IO](duration) *> readAndWrite
      stream.drain.attempt.flatMap {
        case Left(throwable) => Stream.eval(IO.delay(println("error: " + throwable)))
        case Right(value) => Stream(value)
      }.compile.drain
    }.start
    _ <- server[IO].compile.drain
  } yield ExitCode.Success
}