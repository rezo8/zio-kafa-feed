package com.rezo.httpServer.routes

import com.rezo.config.KafkaConsumerConfig
import com.rezo.httpServer.Responses.LoadPeopleResponse
import com.rezo.services.MessageReader
import io.circe.syntax.*
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import zio.ZIO
import zio.http.*

import java.util.Properties
import scala.util.Random

class KafkaRoutes(consumerConfig: KafkaConsumerConfig) extends RouteContainer {
  private val defaultCount = 10

  override def routes: Routes[Any, Response] =
    Routes(
      Method.GET / "topic" / zio.http.string("topicName") / zio.http.int(
        "offset"
      ) -> handler { (topicName: String, offset: Int, req: Request) =>
        {
          val count = req.url
            .queryParams("count")
            .headOption
            .fold(defaultCount)(res => res.toIntOption.getOrElse(defaultCount))

          val requestMessageReader = new MessageReader(consumerConfig)

          val result = for {
            readPeople <- requestMessageReader.processForAllPartitionsZio(
              topicName,
              consumerConfig.partitionList,
              offset,
              count
            )
            response = LoadPeopleResponse(readPeople)
          } yield response

          result.fold(
            error => {
              Response
                .error(Status.InternalServerError, message = error.getMessage)
            },
            resp =>
              Response.json(
                resp
                  .asJson(LoadPeopleResponse.loadPeopleResponseEncoder)
                  .toString
              )
          )
        }
      }
    )

  def cleanUp(): ZIO[Any, Throwable, Unit] = {
    for {
      //      _ <- ZIO.attempt(consumerPool.foreach(x => x.close()))
      _ <- ZIO.logInfo("closed all consumers")
    } yield ()
  }

}
