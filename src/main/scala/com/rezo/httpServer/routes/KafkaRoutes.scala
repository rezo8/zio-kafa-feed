package com.rezo.httpServer.routes

import com.rezo.config.ReaderConfig
import com.rezo.httpServer.Responses.LoadMessagesResponse
import com.rezo.services.{MessageReader, MessageReaderLive}
import io.circe.syntax.*
import zio.http.*
import zio.kafka.admin.AdminClient
import zio.{ZIO, ZLayer}

trait KafkaRoutes {
  def routes: Routes[Any, Response]
}

final case class KafkaRoutesLive(
    adminClient: AdminClient,
    readerConfig: ReaderConfig
) extends KafkaRoutes {
  private val defaultCount = 10

  override def routes: Routes[Any, Response] = Routes(
    Method.GET / "topic" / string("topicName") / int("offset") -> handler {
      (topicName: String, offset: Int, req: Request) =>
        val count = req.url
          .queryParams("count")
          .headOption
          .flatMap(_.toIntOption)
          .getOrElse(defaultCount)
        for {
          res <- handleLoadMessage(topicName, offset, count, readerConfig)
            .provide(
              MessageReaderLive.layer,
              ZLayer.succeed(adminClient)
            )
            .catchAll(error =>
              ZIO.succeed(
                Response.error(
                  Status.InternalServerError,
                  message = error.getMessage
                )
              )
            )
        } yield res
    }
  )

  private def handleLoadMessage(
      topicName: String,
      offset: Int,
      count: Int,
      readerConfig: ReaderConfig
  ): ZIO[
    MessageReader,
    Throwable,
    Response
  ] = {
    for {
      messages <- MessageReaderLive.readMessages(
        readerConfig,
        topicName,
        offset,
        count
      )
      response = Response.json(
        LoadMessagesResponse(messages)
          .asJson(LoadMessagesResponse.loadMessagesResponseEncoder)
          .toString
      )
    } yield response
  }
}

object KafkaRoutesLive {

  val layer: ZLayer[
    AdminClient & ReaderConfig,
    Throwable,
    KafkaRoutes
  ] = {
    ZLayer.fromFunction(KafkaRoutesLive(_, _))
  }

  def make(): ZIO[
    ReaderConfig & AdminClient,
    Nothing,
    KafkaRoutesLive
  ] = {
    for {
      adminClient <- ZIO.service[AdminClient]
      readerConfig <- ZIO.service[ReaderConfig]
    } yield KafkaRoutesLive(adminClient, readerConfig)
  }
}
