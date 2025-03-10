package com.rezo

import com.rezo.config.ServerConfig
import com.rezo.httpServer.routes.KafkaRoutesLive
import org.apache.kafka.clients.consumer.KafkaConsumer
import zio.config.typesafe.TypesafeConfigProvider
import zio.config.typesafe.TypesafeConfigProvider.fromResourcePath
import zio.http.Server
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.{Scope, ZIO, ZIOAppDefault, ZLayer, config, *}

object Main extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.setConfigProvider(fromResourcePath())

  private val configLayer: ZLayer[Any, Throwable, ServerConfig] =
    ZLayer.fromZIO(ZIO.config[ServerConfig].orDie)

  private val adminLayer: ZLayer[ServerConfig & Scope, Throwable, AdminClient] =
    ZLayer.fromZIO {
      for {
        config <- ZIO.service[ServerConfig]
        client <- AdminClient.make(
          AdminClientSettings(config.consumerConfig.bootstrapServers)
        )
      } yield client
    }

  private val appLayer: ZLayer[
    Scope,
    Throwable,
    AdminClient & ServerConfig
  ] = {
    (configLayer >>> adminLayer) ++ configLayer
  }

  override def run: ZIO[Scope, Throwable, Nothing] = {
    ZIO
      .scoped {
        for {
          config <- ZIO.service[ServerConfig]
          kafkaRoutes <- KafkaRoutesLive.make()
          serverProc <- Server
            .serve(kafkaRoutes.routes)
            .provideLayer(
              Server.defaultWithPort(config.serverMetadataConfig.port)
            )
        } yield serverProc
      }
      .provideLayer(appLayer)
  }
}
