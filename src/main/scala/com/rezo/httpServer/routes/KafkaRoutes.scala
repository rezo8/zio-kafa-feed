package com.rezo.httpServer.routes

import com.rezo.Main.ConsumerPool
import com.rezo.config.ReaderConfig
import com.rezo.httpServer.Responses.LoadMessagesResponse
import com.rezo.services.MessageReader
import com.rezo.services.MessageReader.ReadMessageConfig
import io.circe.syntax.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import zio.http.*
import zio.kafka.admin.AdminClient
import zio.{Scope, ZIO, ZLayer, ZPool}

object KafkaRoutes {
  private val defaultCount = 10
  def routes
      : Routes[AdminClient & ConsumerPool & Scope & ReaderConfig, Response] =
    Routes(
      Method.GET / "topic" / zio.http.string("topicName") / zio.http.int(
        "offset"
      ) -> handler { (topicName: String, offset: Int, req: Request) =>
        {
          handleLoadMessage(topicName, offset, req).catchAll(error =>
            ZIO.succeed(
              Response
                .error(Status.InternalServerError, message = error.getMessage)
            )
          )
        }
      }
    )

  private def handleLoadMessage(
      topicName: String,
      offset: Int,
      req: Request
  ): ZIO[
    ConsumerPool & AdminClient & Scope & ReaderConfig,
    Throwable,
    Response
  ] = {
    val count = req.url
      .queryParams("count")
      .headOption
      .fold(defaultCount)(res => res.toIntOption.getOrElse(defaultCount))

    for {
      adminClient <- ZIO.service[AdminClient]
      consumerZPool <- ZIO.service[ConsumerPool]
      readerConfig <- ZIO.service[ReaderConfig]
      partitionCountOpt <- adminClient
        .describeTopics(List(topicName))
        .map(_.get(topicName).map(_.partitions.map(_.partition)))
      partitions <- ZIO.attempt(partitionCountOpt.get)
      partitionGroups = partitions
        .grouped(
          (partitions.size + readerConfig.parallelReads - 1) / readerConfig.parallelReads
        )
        .toSeq
      allMessages <- ZIO
        .foreachPar(partitionGroups)(partitionGroup =>
          ZIO.scoped {
            for {
              consumer <- consumerZPool.get
              readMessageConfig = ReadMessageConfig(
                topicName,
                partitionGroup,
                offset,
                count
              )
              readMessages <- MessageReader.readMessagesForPartitions
                .provideLayer(
                  ZLayer.succeed(consumer) ++ ZLayer.succeed(readMessageConfig)
                )
            } yield readMessages
          }
        )
        .map(_.flatten)
      res = Response.json(
        LoadMessagesResponse(allMessages)
          .asJson(LoadMessagesResponse.loadMessagesResponseEncoder)
          .toString
      )
    } yield res
  }
}
