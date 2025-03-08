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
import zio.{Scope, ZIO, ZLayer}

object KafkaRoutes {
  private val defaultCount = 10

  def routes
      : Routes[AdminClient & ConsumerPool & ReaderConfig & Scope, Response] =
    Routes(
      Method.GET / "topic" / string("topicName") / int("offset") -> handler {
        (topicName: String, offset: Int, req: Request) =>
          val count = req.url
            .queryParams("count")
            .headOption
            .flatMap(_.toIntOption)
            .getOrElse(defaultCount)
          handleLoadMessage(topicName, offset, count).catchAll(error =>
            ZIO.succeed(
              Response.error(
                Status.InternalServerError,
                message = error.getMessage
              )
            )
          )
      }
    )

  private def handleLoadMessage(
      topicName: String,
      offset: Int,
      count: Int
  ): ZIO[
    AdminClient & ConsumerPool & ReaderConfig & Scope,
    Throwable,
    Response
  ] = {
    for {
      adminClient <- ZIO.service[AdminClient]
      consumerPool <- ZIO.service[ConsumerPool]
      readerConfig <- ZIO.service[ReaderConfig]
      partitions <- adminClient
        .describeTopics(List(topicName))
        .map(_.get(topicName).map(_.partitions.map(_.partition)))
        .someOrFail(new RuntimeException(s"Topic $topicName not found"))
      partitionGroups = partitions
        .grouped(
          (partitions.size + readerConfig.parallelReads - 1) / readerConfig.parallelReads
        )
        .toSeq
      allMessages <- ZIO
        .foreachPar(partitionGroups) { partitionGroup =>
          ZIO.scoped {
            for {
              // Note. The ZPool handles the release of resources automatically. So this Get doesn't lead to leaks
              consumer <- consumerPool.get
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
        }
        .map(_.flatten)
      response = Response.json(
        LoadMessagesResponse(allMessages)
          .asJson(LoadMessagesResponse.loadMessagesResponseEncoder)
          .toString
      )
    } yield response
  }
}
