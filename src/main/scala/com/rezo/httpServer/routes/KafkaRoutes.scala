package com.rezo.httpServer.routes

import zio.http.*

class KafkaRoutes extends RouteContainer {

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
            .flatMap(_.toIntOption)
            .getOrElse(defaultCount)

          Response.text(
            s"received topicName [$topicName] with offset [$offset] and count [$count]"
          )
        }
      }
    )
}
