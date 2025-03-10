package com.rezo.services.ingestion

import com.rezo.config.IngestionJobConfig
import com.rezo.objects.Person
import org.apache.kafka.clients.producer.ProducerRecord
import zio.kafka.producer.Producer
import zio.kafka.serde.Serde
import zio.stream.ZStream
import zio.{Clock, ZIO, ZLayer}

import java.util.concurrent.TimeUnit

trait DataPublisher {
  def publishPeople(people: List[Person]): ZIO[Any, Throwable, Unit]
}

private final case class DataPublisherLive(
    producer: Producer,
    config: IngestionJobConfig
) extends DataPublisher {

  override def publishPeople(people: List[Person]): ZIO[Any, Throwable, Unit] =
    for {
      startTime <- Clock.currentTime(TimeUnit.MILLISECONDS)
      successfulPublishes <- ZStream
        .fromIterable(as = people, chunkSize = config.batchSize)
        .map(person => {
          ProducerRecord(
            config.producerConfig.topicName,
            person._id,
            person
          )
        })
        .via(producer.produceAll(Serde.string, Person.serde))
        .tapError(e =>
          ZIO.logError(s"Failed to publish record: ${e.getMessage}")
        )
        .runFold(0) { (acc, _) => acc + 1 }
      endTime <- Clock.currentTime(TimeUnit.MILLISECONDS)
      _ <- ZIO.logInfo(
        s"Successfully published $successfulPublishes records in ${endTime - startTime} ms to topic ${config.producerConfig.topicName}."
      )
    } yield ()
}

object DataPublisherLive {
  val layer: ZLayer[Producer & IngestionJobConfig, Throwable, DataPublisher] = {
    ZLayer.fromFunction(DataPublisherLive(_, _))
  }

  def publishPeople(
      people: List[Person]
  ): ZIO[DataPublisher, Throwable, Unit] =
    ZIO.serviceWithZIO(_.publishPeople(people))
}
