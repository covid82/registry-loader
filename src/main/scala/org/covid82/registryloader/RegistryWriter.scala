package org.covid82.registryloader

import cats.effect.{Async, Blocker, ContextShift}
import doobie.util.update.Update
import fs2.{Pipe, Stream}
import cats.syntax.apply._
import cats.instances.list._
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux

object RegistryWriter {

  private def xa[F[_] : Async : ContextShift](
    service: String,
    user: String,
    pass: String
  ): Aux[F, Unit] = Transactor.fromDriverManager[F](
    driver = "org.postgresql.Driver",
    url = s"jdbc:postgresql://$service/registry?protocolVersion=3&stringtype=unspecified&socketTimeout=300&tcpKeepAlive=true",
    user = user,
    pass = pass,
    blocker = Blocker.liftExecutionContext(ExecutionContexts.synchronous)
  )

  private val drop = sql" DROP TABLE IF EXISTS row ".update.run

  private val create =
    sql"""
            CREATE TABLE row (
              id         SERIAL,
              registry   VARCHAR,
              cc         VARCHAR,
              typ        VARCHAR,
              start      VARCHAR,
              value      VARCHAR,
              date       VARCHAR,
              status     VARCHAR,
              extensions VARCHAR
            )
             """.update.run

  private val insert =
    """
      | insert into row (registry, cc, typ, start, value, date, status, extensions)
      | values (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin

  def write[F[_] : Async : ContextShift](
    service: String,
    user: String,
    pass: String
  ): Pipe[F, RipeRecord, Int] = in => Stream.eval[F, Int] {
    (drop, create).mapN(_ + _).transact(xa[F](service, user, pass))
  } *> in.chunkN(8096).map(_.toList).evalMap { rows =>
    Update[RipeRecord](insert).updateMany(rows).transact(xa[F](service, user, pass))
  }
}