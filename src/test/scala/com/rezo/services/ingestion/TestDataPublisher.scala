package com.rezo.services.ingestion

import io.circe.Decoder
import zio.kafka.serde.Serde
import zio.{Ref, UIO, ZIO, ZLayer}

final case class PublisherState(data: Either[Throwable, Unit])
object PublisherState {
  val empty = PublisherState(Right(()))
}

class TestDataPublisher(state: Ref[PublisherState]) extends DataPublisher {

  def overwriteState(data: Either[Throwable, Unit]): UIO[Unit] = {
    state.update(_ => new PublisherState(data))
  }

  def customPublish[A](
      data: List[A],
      topic: String,
      batchSize: Int,
      customSerde: Serde[Any, A],
      getId: A => String
  ): ZIO[Any, Throwable, Unit] = state.get.flatMap(_.data match {
    case Left(error) => ZIO.fail(error)
    case Right(_)    => ZIO.succeed(())
  })
}

object TestDataPublisher {
  val test: ZLayer[Any, Nothing, TestDataPublisher] =
    ZLayer {
      for {
        ref <- Ref.make(PublisherState.empty)
      } yield new TestDataPublisher(ref)
    }

  def overwriteState(
      data: Either[Throwable, Unit]
  ): ZIO[TestDataPublisher, Nothing, Unit] =
    ZIO.serviceWithZIO(_.overwriteState(data))

}
