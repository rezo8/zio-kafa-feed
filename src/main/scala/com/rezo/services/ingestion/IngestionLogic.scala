package com.rezo.services.ingestion

import com.rezo.config.IngestionJobConfig
import com.rezo.objects.Person
import io.circe.Decoder
import zio.kafka.serde.Serde
import zio.{ZIO, ZLayer}

import java.util.UUID

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
      _ <- ingest[Person](
        Person.personDecoder,
        Person.serde,
        (p: Person) => p._id
      )
    } yield ()
  }

  private def ingest[A](
      decoder: Decoder[A],
      serde: Serde[Any, A],
      getId: A => String = (a: A) =>
        UUID
          .randomUUID()
          .toString
  ) = {
    for {
      decodedResults <- dataFetcher.fetchData[A](
        ingestionJobConfig.filePath
      )(decoder)
      decoded = decodedResults.collect { case Right(x) => x }

      publishResult <- dataPublisher.customPublish[A](
        decoded,
        ingestionJobConfig.producerConfig.topicName,
        ingestionJobConfig.batchSize,
        serde,
        getId
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
