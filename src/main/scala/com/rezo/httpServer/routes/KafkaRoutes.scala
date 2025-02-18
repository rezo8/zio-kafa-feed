package com.rezo.httpServer.routes

import com.rezo.config.KafkaConsumerConfig
import com.rezo.httpServer.Responses.LoadPeopleResponse
import com.rezo.services.MessageReader
import io.circe.syntax.*
import zio.http.*

class KafkaRoutes(consumerConfig: KafkaConsumerConfig) extends RouteContainer {
  private val defaultCount = 10
  private val messageReader = new MessageReader(consumerConfig)

  override def routes: Routes[Any, Response] =
    Routes(
      Method.GET / "topic" / zio.http.string("topicName") / zio.http.int(
        "offset"
      ) -> handler { (topicName: String, offset: Int, req: Request) =>
        {
          val count = req.url
            .queryParams("count")
            .headOption
            .flatMap(_.toIntOption)
            .getOrElse(defaultCount)

          val result = for {
            readPeople <- messageReader.processForAllPartitionsZio(
              topicName,
              consumerConfig.partitionList,
              10,
              10
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
}
