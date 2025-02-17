package com.rezo

import com.rezo.config.{DerivedConfig, IngestionJobConfig}
import com.rezo.exceptions.Exceptions.ConfigLoadException
import com.rezo.kafka.PeoplePublisher
import com.rezo.objects.{CtRoot, Person}
import io.circe.parser.*
import io.circe.{Decoder, Error}
import pureconfig.ConfigSource
import zio.*
import zio.stream.*

object IngestionJob extends ZIOAppDefault {

  val config: IngestionJobConfig = ConfigSource.default
    .at("ingestion-job")
    .load[DerivedConfig]
    .getOrElse(throw new ConfigLoadException())
    .asInstanceOf[IngestionJobConfig]

  private val filePath = "random-people-data.json"

  private val peoplePublisher = new PeoplePublisher(config.publisherConfig)

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

      _ <- validPersons.flatMap()
      // TODO publish to kafka
    } yield (1)
  }
}
