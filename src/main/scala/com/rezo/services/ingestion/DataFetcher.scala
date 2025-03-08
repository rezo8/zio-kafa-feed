package com.rezo.services.ingestion

import com.rezo.exceptions.Exceptions.{FailedToParseFile, FailedToReadFile}
import com.rezo.objects.{CtRoot, Person}
import io.circe.parser.*
import zio.*
import zio.stream.*

trait DataFetcher {
  def fetchPeople(): ZIO[Any, Throwable, List[Person]]
}

type FilePath = String

final case class DataFetcherLive(filePath: FilePath) extends DataFetcher {
  override def fetchPeople(): ZIO[Any, Throwable, List[Person]] = {
    for {
      jsonString <- ZStream
        .fromResource(filePath)
        .via(ZPipeline.utf8Decode)
        .runCollect
        .map(_.mkString)
        .mapError(e => FailedToReadFile(e))
      parsedPeople <- ZIO
        .fromEither(parse(jsonString).flatMap(_.as[CtRoot]))
        .mapError(e => FailedToParseFile(e))
        .map(_.ctRoot.map(_.as[Person]))
      people = parsedPeople.collect { case Right(person) => person }
    } yield people
  }
}

object DataFetcherLive {
  val layer: ZLayer[FilePath, Throwable, DataFetcher] = {
    ZLayer.fromFunction(DataFetcherLive(_))
  }

  def fetchPeople(): ZIO[DataFetcher, Throwable, List[Person]] =
    ZIO.serviceWithZIO(_.fetchPeople())
}
