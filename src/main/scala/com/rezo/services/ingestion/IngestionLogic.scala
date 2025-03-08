package com.rezo.services.ingestion

import zio.{ZIO, ZLayer}

trait IngestionLogic {
  def run: ZIO[Any, Throwable, Unit]
}

private final case class IngestionLogicLive(
    dataFetcher: DataFetcher,
    dataPublisher: DataPublisher
) extends IngestionLogic {

  override def run: ZIO[Any, Throwable, Unit] = {
    for {
      people <- dataFetcher.fetchPeople()
      publishResult <- dataPublisher.publishPeople(people)
    } yield ()
  }
}

object IngestionLogicLive {
  val layer: ZLayer[DataFetcher & DataPublisher, Throwable, IngestionLogic] = {
    ZLayer.fromFunction(IngestionLogicLive(_, _))
  }

  def run: ZIO[IngestionLogic, Throwable, Unit] =
    ZIO.serviceWithZIO(_.run)
}
