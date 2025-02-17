package com.rezo

import com.rezo.objects.{Person, CtRoot}
import io.circe.parser.*
import io.circe.{Decoder, Error}
import zio.*
import zio.stream.*

object IngestionJob extends ZIOAppDefault {

  private val filePath = "random-people-data.json"

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
      // TODO publish to kafka
    } yield (1)
  }
}
