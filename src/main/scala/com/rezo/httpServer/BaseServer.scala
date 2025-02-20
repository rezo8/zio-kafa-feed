package com.rezo.httpServer

import com.rezo.config.ServerMetadataConfig
import com.rezo.httpServer.routes.KafkaRoutes
import zio.http.Server
import zio.{ZIO, ZIOAppDefault}

trait BaseServer extends ZIOAppDefault {
  val serverMetadataConfig: ServerMetadataConfig
  val kafkaRoutes: KafkaRoutes

  def startServer: ZIO[Any, Throwable, Nothing] = {
    Server
      .serve(kafkaRoutes.routes)
      .provide(Server.defaultWithPort(serverMetadataConfig.port))
  }

}
