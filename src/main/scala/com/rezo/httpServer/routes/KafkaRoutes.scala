package com.rezo.httpServer.routes

import com.rezo.config.{KafkaConsumerConfig, ReaderConfig}
import com.rezo.httpServer.Responses.LoadMessagesResponse
import com.rezo.kafka.KafkaConsumerFactory
import com.rezo.services.MessageReader
import io.circe.syntax.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import zio.http.*
import zio.{URIO, ZIO, ZLayer}

import scala.util.Random

class KafkaRoutes(
    consumerConfig: KafkaConsumerConfig,
    readerConfig: ReaderConfig
) extends RouteContainer {
  private val defaultCount = 10
  private val consumerPool
      : Seq[ZLayer[Any, Throwable, KafkaConsumer[String, String]]] =
    (0 to readerConfig.consumerCount).map(_ =>
      for {
        layer <- KafkaConsumerFactory.make(consumerConfig)
      } yield layer
    )

  override def routes: Routes[Any, Response] =
    Routes(
      Method.GET / "topic" / zio.http.string("topicName") / zio.http.int(
        "offset"
      ) -> handler { (topicName: String, offset: Int, req: Request) =>
        {
          handleLoadMessage(topicName, offset, req)
        }
      }
    )

  private def handleLoadMessage(
      topicName: String,
      offset: Int,
      req: Request
  ): URIO[Any, Response] = {
    val count = req.url
      .queryParams("count")
      .headOption
      .fold(defaultCount)(res => res.toIntOption.getOrElse(defaultCount))
    val requestMessageReader = new MessageReader(consumerConfig)
    val result = for {
      readMessages <- requestMessageReader
        .readMessagesForPartitions(
          topicName,
          consumerConfig.partitionList,
          offset,
          count
        )
        .provideLayer(consumerPool(Random.nextInt(consumerPool.size)))
      response = LoadMessagesResponse(readMessages)
    } yield response

    result
      .fold(
        error => {
          Response
            .error(Status.InternalServerError, message = error.getMessage)
        },
        resp =>
          Response.json(
            resp
              .asJson(LoadMessagesResponse.loadMessagesResponseEncoder)
              .toString
          )
      )
  }

  def cleanUp(): ZIO[Any, Throwable, Unit] = {
    for {
      _ <- ZIO.logInfo("closed all consumers")
    } yield ()
  }

}
