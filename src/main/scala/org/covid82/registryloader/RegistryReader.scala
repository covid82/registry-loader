package org.covid82.registryloader

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect.{Async, ContextShift, Resource, Sync}
import fs2.Stream

import scala.concurrent.{ExecutionContext => EC, ExecutionContextExecutor => ECE}

trait RegistryReader[F[_]] {

  protected def read(implicit ec: ECE, cs: ContextShift[F], a: Async[F]): Stream[F, RipeRecord]

  private def executionContext(implicit F: Sync[F]): Resource[F, (ExecutorService, ECE)] =
    Resource.make(F.delay {
      val ec: ExecutorService = Executors.newSingleThreadExecutor()
      val ece: ECE = EC.fromExecutor(ec)
      (ec, ece)
    }) { case (service, _) => F.delay(service.shutdown()) }

  def readRows(implicit async: Async[F], cs: ContextShift[F]): Stream[F, RipeRecord] = for {
    ec <- Stream.resource(executionContext).map { case (_, ec) => ec }
    rs <- read(ec, cs, async)
      .filter(r => Set("ipv4" /*, "ipv6"*/).contains(r.typ))
  } yield rs
}

object RegistryReader {
  def ftp[F[_]](config: FtpConfig): RegistryReader[F] = new FtpRegistryReader[F](config)
}