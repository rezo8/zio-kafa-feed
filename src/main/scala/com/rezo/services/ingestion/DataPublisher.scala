package com.rezo.services.ingestion

import org.apache.kafka.clients.producer.ProducerRecord
import zio.kafka.producer.Producer
import zio.kafka.serde.Serde
import zio.stream.ZStream
import zio.{Clock, ZIO, ZLayer}

import java.util.UUID
import java.util.concurrent.TimeUnit

trait DataPublisher {
  def customPublish[A](
      data: List[A],
      topic: String,
      batchSize: Int,
      customSerde: Serde[Any, A],
      getId: A => String
  ): ZIO[Any, Throwable, Unit]
}

private final case class DataPublisherLive(producer: Producer)
    extends DataPublisher {
  def customPublish[A](
      data: List[A],
      topic: String,
      batchSize: Int,
      customSerde: Serde[Any, A],
      getId: A => String = (a: A) => UUID.randomUUID().toString
  ): ZIO[Any, Throwable, Unit] = for {
    startTime <- Clock.currentTime(TimeUnit.MILLISECONDS)
    successfulPublishes <- ZStream
      .fromIterable(as = data, chunkSize = batchSize)
      .map(data => {
        ProducerRecord(
          topic,
          getId(data),
          data
        )
      })
      .via(producer.produceAll(Serde.string, customSerde))
      .tapError(e => ZIO.logError(s"Failed to publish record: ${e.getMessage}"))
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

  def customPublish[A](
      data: List[A],
      topic: String,
      batchSize: Int,
      customSerde: Serde[Any, A],
      getId: A => String = (a: A) => UUID.randomUUID().toString
  ): ZIO[DataPublisher, Throwable, Unit] =
    ZIO.serviceWithZIO(
      _.customPublish(data, topic, batchSize, customSerde, getId)
    )

}
