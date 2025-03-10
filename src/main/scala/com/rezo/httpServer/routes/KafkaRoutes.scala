package com.rezo.httpServer.routes

import com.rezo.config.{ReaderConfig, ServerConfig}
import com.rezo.httpServer.Responses.LoadMessagesResponse
import com.rezo.kafka.KafkaClientFactory
import com.rezo.services.{MessageReader, MessageReaderLive}
import com.rezo.util.HelperTypes.ConsumerPool
import io.circe.syntax.*
import zio.http.*
import zio.kafka.admin.AdminClient
import zio.{Scope, ZIO, ZLayer, ZPool}

trait KafkaRoutes {
  def routes: Routes[Any, Response]
}

final case class KafkaRoutesLive(
    adminClient: AdminClient,
    consumerPool: ConsumerPool,
    config: ServerConfig
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
          res <- handleLoadMessage(
            topicName,
            offset,
            count,
            config.readerConfig
          )
            .provide(
              MessageReaderLive.layer,
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
    AdminClient & ServerConfig & ConsumerPool,
    Throwable,
    KafkaRoutes
  ] = {
    ZLayer.fromFunction(KafkaRoutesLive(_, _, _))
  }

  // TODO potentially add ConsumerPool as a dependency.
  def make(): ZIO[
    ServerConfig & Scope & AdminClient,
    Nothing,
    KafkaRoutesLive
  ] = {
    for {
      config <- ZIO.service[ServerConfig]
      adminClient <- ZIO.service[AdminClient]
      consumerPool <- ZPool.make(
        KafkaClientFactory.makeKafkaConsumer(config.consumerConfig),
        config.readerConfig.consumerCount
      )
    } yield KafkaRoutesLive(adminClient, consumerPool, config)
  }
}
