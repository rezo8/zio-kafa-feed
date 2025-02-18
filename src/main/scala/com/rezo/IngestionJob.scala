package com.rezo

import com.rezo.config.{DerivedConfig, IngestionJobConfig}
import com.rezo.exceptions.Exceptions.{ConfigLoadException, PublishError}
import com.rezo.objects.{CtRoot, Person}
import io.circe.Error
import io.circe.parser.*
import pureconfig.ConfigSource
import zio.*
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.stream.*

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

  private def produce(
      personEither: Either[io.circe.Error, Person]
  ): ZIO[Any, Throwable, Int] = {
    personEither match {
      case Right(person) =>
        Producer
          .produce(
            topic = config.publisherConfig.topicName,
            key = person._id,
            value = person,
            keySerializer = Serde.string,
            valueSerializer = Person.serde
          )
          .mapError(x => {
            println(x.getMessage)
            PublishError(x)
          })
          .map(x =>
            println(
              s"Successfully published to ${x.offset()} on partition ${x.partition()}"
            )
          )
          .as(1)
          .provideLayer(producer)
      case Left(_) => ZIO.succeed(0)
    }
  }

  private def loadJson: ZIO[Any, Throwable, List[Either[Error, Person]]] = for {
    jsonString <- ZStream
      .fromResource(filePath)
      .via(ZPipeline.utf8Decode)
      .runCollect
      .map(_.mkString)

    people <- ZIO
      .fromEither(parse(jsonString).flatMap(_.as[CtRoot]))
      .mapError(e =>
        new RuntimeException(s"Failed to parse JSON array: ${e.getMessage}")
      )
      .map(_.ctRoot.map(_.as[Person]))
  } yield people

  override def run: ZIO[Any, Throwable, Int] = {
    for {
      persons <- loadJson
      (validPersons, invalidPersons) = persons.partition(_.isRight)
      _ <- ZIO.foreachDiscard(validPersons)(
        produce
      ) // this is a nice method because it doesn't fail if one fails.
    } yield (1)
  }
}
