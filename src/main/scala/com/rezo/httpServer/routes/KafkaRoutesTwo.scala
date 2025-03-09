package com.rezo.httpServer.routes

import com.rezo.config.ReaderConfig
import com.rezo.httpServer.Responses.LoadMessagesResponse
import com.rezo.services.{
  ConsumerPoolTwo,
  MessageReaderTwo,
  MessageReaderTwoLive
}
import io.circe.syntax.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import zio.http.*
import zio.kafka.admin.AdminClient
import zio.{ZIO, ZLayer}

trait KafkaRoutesTwo {
  def routes: Routes[Any, Response]
}

final case class KafkaRoutesTwoLive(
    adminClient: AdminClient,
    consumerPool: ConsumerPoolTwo,
    readerConfig: ReaderConfig
) extends KafkaRoutesTwo {
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
              MessageReaderTwoLive.layer,
              ZLayer.succeed(adminClient),
              ZLayer.succeed(consumerPool)
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
    MessageReaderTwo,
    Throwable,
    Response
  ] = {
    for {
      messages <- MessageReaderTwoLive.readMessages(
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

object KafkaRoutesTwoLive {

  val layer: ZLayer[
    AdminClient & ConsumerPoolTwo & ReaderConfig,
    Throwable,
    KafkaRoutesTwo
  ] = {
    ZLayer.fromFunction(KafkaRoutesTwoLive(_, _, _))
  }

  def make(): ZIO[
    ReaderConfig & ConsumerPoolTwo & AdminClient,
    Nothing,
    KafkaRoutesTwoLive
  ] = {
    for {
      adminClient <- ZIO.service[AdminClient]
      consumerPool <- ZIO.service[ConsumerPoolTwo]
      readerConfig <- ZIO.service[ReaderConfig]
    } yield KafkaRoutesTwoLive(adminClient, consumerPool, readerConfig)
  }
}
