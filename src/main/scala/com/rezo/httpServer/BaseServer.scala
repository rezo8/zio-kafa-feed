package com.rezo.httpServer

import com.rezo.config.ServerMetadataConfig
import com.rezo.httpServer.routes.KafkaRoutes
import zio.ZIO
import zio.http.Server

trait BaseServer {
  val serverMetadataConfig: ServerMetadataConfig
  val kafkaRoutes: KafkaRoutes

  def startServer: ZIO[Any, Throwable, Nothing] = {
    Server
      .serve(kafkaRoutes.routes)
      .provide(Server.defaultWithPort(serverMetadataConfig.port))
  }

}
