package com.rezo.services.ingestion

import com.rezo.exceptions.Exceptions.{FailedToParseFile, FailedToReadFile}
import com.rezo.objects.CtRoot
import io.circe.Decoder
import io.circe.parser.*
import zio.*
import zio.stream.*

trait DataFetcher {
  def fetchData[A](
      filePath: String
  )(implicit decoder: Decoder[A]): ZIO[Any, Throwable, List[Decoder.Result[A]]]
}

final case class DataFetcherLive() extends DataFetcher {
  override def fetchData[A](
      filePath: String
  )(implicit
      decoder: Decoder[A]
  ): ZIO[Any, Throwable, List[Decoder.Result[A]]] = {
    for {
      jsonString <- ZStream
        .fromResource(filePath)
        .via(ZPipeline.utf8Decode)
        .runCollect
        .map(_.mkString)
        .mapError(e => FailedToReadFile(e))
      parsedObjects <- ZIO
        .fromEither(parse(jsonString).flatMap(_.as[CtRoot]))
        .mapError(e => FailedToParseFile(e))
        .map(_.ctRoot.map(_.as[A]))
    } yield parsedObjects
  }
}

object DataFetcherLive {
  val layer: ZLayer[Any, Throwable, DataFetcher] = {
    ZLayer.succeed(DataFetcherLive())
  }

  def fetchData[A](filePath: String)(implicit
      decoder: Decoder[A]
  ): ZIO[DataFetcher, Throwable, List[Decoder.Result[A]]] =
    ZIO.serviceWithZIO(_.fetchData(filePath)(decoder))
}
