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
  def publishPeople(
      people: List[Person],
      topic: String,
      batchSize: Int
  ): ZIO[Any, Throwable, Unit]
}

private final case class DataPublisherLive(producer: Producer)
    extends DataPublisher {

  override def publishPeople(
      people: List[Person],
      topic: String,
      batchSize: Int
  ): ZIO[Any, Throwable, Unit] =
    for {
      startTime <- Clock.currentTime(TimeUnit.MILLISECONDS)
      successfulPublishes <- ZStream
        .fromIterable(as = people, chunkSize = batchSize)
        .map(person => {
          ProducerRecord(
            topic,
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
        s"Successfully published $successfulPublishes records in ${endTime - startTime} ms to topic ${topic}."
      )
    } yield ()
}

object DataPublisherLive {
  val layer: ZLayer[Producer, Throwable, DataPublisher] = {
    ZLayer.fromFunction(DataPublisherLive(_))
  }

  def publishPeople(
      people: List[Person],
      topic: String,
      batchSize: Int
  ): ZIO[DataPublisher, Throwable, Unit] =
    ZIO.serviceWithZIO(_.publishPeople(people, topic, batchSize))
}
