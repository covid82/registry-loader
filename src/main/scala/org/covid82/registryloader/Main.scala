package org.covid82.registryloader

import cats.effect.{Async, ContextShift, ExitCode, IO, IOApp, Resource, Sync, Timer}
import cats.kernel.Monoid
import org.covid82.registryloader.RegistryWriter.write
import fs2.Stream._
import cats.kernel.instances.int.catsKernelStdGroupForInt
import fs2.Stream
import cats.syntax.apply._
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}

object Main extends IOApp {

  val ftpConfig: FtpConfig = FtpConfig(
    path = "/pub/stats/ripencc/delegated-ripencc-latest",
    host = "ftp.ripe.net", port = 21,
    user = "anonymous", pass = ""
  )

  def log[F[_] : Sync](msg: String): Stream[F, Unit] = eval[F, Unit](Sync[F].delay(println(msg)))

  def count[F[_], A](fa: Stream[F, A]): Stream[F, Int] = fa.foldMap(_ => 1)

  def sum[F[_], A: Monoid](s: Stream[F, A]): Stream[F, A] = s.foldMonoid

  def load[F[_] : Timer : Async : ContextShift](
    service: String,
    user: String,
    pass: String
  ): Stream[F, Unit] = {
    val loader = RegistryReader.ftp[F](ftpConfig)
    log("started loading registry") *> {
      val rows = loader.readRows
      count(rows).flatMap(size => log(s"loaded $size records")) *> {
        sum(rows.through(write(service, user, pass))).flatMap { count =>
          log(s"saved $count records into database")
        }
      }.drain
    }
  }

  override def run(args: List[String]): IO[ExitCode] = for {
    (service, user, pass) <- IO.delay {
      val service = sys.env.getOrElse("DB_SERVICE", "localhost:5432")
      val user = sys.env.getOrElse("DB_USER", "postgres")
      val pass = sys.env.getOrElse("DB_PASS", "postgres")
      (service, user, pass)
    }
    _ <- IO {
      println(s"service: [$service]")
      println(s"user:    [$user]")
      println(s"pass:    [******]")
    }
    _ <- {
      load[IO](service, user, pass).drain.attempt.flatMap {
        case Left(throwable) => Stream.eval(IO.delay(println("error: " + throwable)))
        case Right(value) => Stream(value)
      }.compile.drain
    }
    _ <- Resource[IO, AmazonSQS](IO((AmazonSQSClientBuilder.defaultClient, IO.unit))).use { sqs =>
      IO(sqs.sendMessage("registry-notification", "{\"version\":" + System.currentTimeMillis() + "}"))
    }

  } yield ExitCode.Success
}