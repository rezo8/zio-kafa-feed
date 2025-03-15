package com.rezo.services.ingestion

import com.rezo.config.IngestionJobConfig
import com.rezo.objects.Person
import zio.{ZIO, ZLayer}

trait IngestionLogic {
  def run: ZIO[Any, Throwable, Unit]
}

private final case class IngestionLogicLive(
    ingestionJobConfig: IngestionJobConfig,
    dataFetcher: DataFetcher,
    dataPublisher: DataPublisher
) extends IngestionLogic {

  override def run: ZIO[Any, Throwable, Unit] = {
    for {
      decodedPeopleResults <- dataFetcher.fetchData[Person](
        ingestionJobConfig.filePath
      )
      decodedPeople = decodedPeopleResults.collect { case Right(person) =>
        person
      }

      publishResult <- dataPublisher.publishPeople(
        decodedPeople,
        ingestionJobConfig.producerConfig.topicName,
        ingestionJobConfig.batchSize
      )
    } yield ()
  }
}

object IngestionLogicLive {
  val layer: ZLayer[
    IngestionJobConfig & DataFetcher & DataPublisher,
    Throwable,
    IngestionLogic
  ] = {
    ZLayer.fromFunction(IngestionLogicLive(_, _, _))
  }

  def run: ZIO[IngestionLogic, Throwable, Unit] =
    ZIO.serviceWithZIO(_.run)
}
