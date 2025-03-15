package com.rezo.services.ingestion

import io.circe.Decoder
import zio.{Ref, UIO, ZIO, ZLayer}

final case class FetcherState(data: Either[Throwable, List[String]])
object FetcherState {
  val empty = FetcherState(Right(List[String]()))
}

class TestDataFetcher(state: Ref[FetcherState]) extends DataFetcher {

  def overwriteState(data: Either[Throwable, List[String]]): UIO[Unit] = {
    state.update(_ => new FetcherState(data))
  }

  def fetchData[A](
      filePath: String
  )(implicit
      decoder: Decoder[A]
  ): ZIO[Any, Throwable, List[Decoder.Result[A]]] =
    state.get.flatMap(_.data match {
      case Left(error) => ZIO.fail(error)
      case Right(records) =>
        ZIO.succeed(records.map(value => Right(value.asInstanceOf[A])))
    })
}

object TestDataFetcher {
  val test: ZLayer[Any, Nothing, TestDataFetcher] =
    ZLayer {
      for {
        ref <- Ref.make(FetcherState.empty)
      } yield new TestDataFetcher(ref)
    }

  def overwriteState(
      data: Either[Throwable, List[String]]
  ): ZIO[TestDataFetcher, Nothing, Unit] =
    ZIO.serviceWithZIO(_.overwriteState(data))

  def fetchData(
      filePath: String
  ): ZIO[TestDataFetcher, Throwable, List[Decoder.Result[String]]] =
    ZIO.serviceWithZIO(_.fetchData[String](filePath))

}
