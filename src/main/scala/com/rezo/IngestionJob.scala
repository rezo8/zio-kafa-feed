package com.rezo

import com.rezo.config.{DerivedConfig, IngestionJobConfig}
import com.rezo.exceptions.Exceptions.ConfigLoadException
import com.rezo.objects.{CtRoot, Person}
import io.circe.Error
import io.circe.parser.*
import org.apache.kafka.clients.producer.ProducerRecord
import pureconfig.ConfigSource
import zio.*
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.stream.*

import java.util.concurrent.TimeUnit

object IngestionJob extends ZIOAppDefault {
  val config: IngestionJobConfig = ConfigSource.default
    .at("ingestion-job")
    .load[DerivedConfig]
    .getOrElse(throw new ConfigLoadException())
    .asInstanceOf[IngestionJobConfig]

  private val filePath = "random-people-data.json"

  private val producer: ZLayer[Any, Throwable, Producer] =
    ZLayer.scoped(
      Producer.make(ProducerSettings(config.publisherConfig.bootstrapServers))
    )

  private def loadJson: ZIO[Any, Throwable, List[Either[Error, Person]]] = for {
    jsonString <- ZStream
      .fromResource(filePath)
      .via(ZPipeline.utf8Decode)
      .runCollect
      .map(_.mkString)
      .tapError(e => ZIO.logError(s"Failed to load JSON file: ${e.getMessage}"))

    people <- ZIO
      .fromEither(parse(jsonString).flatMap(_.as[CtRoot]))
      .mapError(e => {
        ZIO.logError(s"Failed to parse JSON array: ${e.getMessage}")
        new RuntimeException(s"Failed to parse JSON array: ${e.getMessage}")
      })
      .map(_.ctRoot.map(_.as[Person]))
  } yield people

  override def run: ZIO[Any, Throwable, Unit] = {
    (for {
      persons <- loadJson
      validPersons = persons.collect { case Right(person) => person }
      _ <- ZIO.logInfo(
        s"Successfully loaded ${validPersons.size} valid records from the file."
      )
      producer <- ZIO.service[Producer]
      startTime <- Clock.currentTime(TimeUnit.MILLISECONDS)
      successfulPublishes <- ZStream
        .fromIterable(as = validPersons, chunkSize = config.batchSize)
        .map(person => {
          ProducerRecord(
            config.publisherConfig.topicName,
            person._id,
            person
          )
        })
        .via(producer.produceAll(Serde.string, Person.serde))
        .tapError(e =>
          ZIO.logError(s"Failed to publish record: ${e.getMessage}")
        )
        .runFold(0) { (acc, _) =>
          acc + 1
        }
      endTime <- Clock.currentTime(TimeUnit.MILLISECONDS)
      _ <- ZIO.logInfo(
        s"Successfully published $successfulPublishes records in ${endTime - startTime} ms."
      )
    } yield ())
      .provideLayer(producer)
      .catchAll(e => ZIO.logError(s"Job failed with error: ${e.getMessage}"))
  }
}
