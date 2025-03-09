package com.rezo

import com.rezo.config.{
  DerivedConfig,
  ReaderConfig,
  ServerConfig,
  ServerMetadataConfig
}
import com.rezo.exceptions.Exceptions.ConfigLoadException
import com.rezo.httpServer.routes.KafkaRoutesLive
import org.apache.kafka.clients.consumer.KafkaConsumer
import pureconfig.ConfigSource
import zio.http.Server
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.{Scope, ZIO, ZIOAppDefault, ZLayer, ZPool}

object Main extends ZIOAppDefault {

  val config: ServerConfig = ConfigSource.default
    .at("server")
    .load[DerivedConfig]
    .getOrElse(throw new ConfigLoadException())
    .asInstanceOf[ServerConfig]

  private val serverMetadataConfig: ServerMetadataConfig =
    config.serverMetadataConfig

  private val adminLayer: ZLayer[Any, Throwable, AdminClient] =
    ZLayer.scoped {
      AdminClient.make(
        AdminClientSettings.apply(config.consumerConfig.bootstrapServers)
      )
    }

  type ConsumerPool = ZPool[Throwable, KafkaConsumer[String, String]]

  val appLayer: ZLayer[
    Scope,
    Throwable,
    AdminClient & ReaderConfig & Server & Scope
  ] = {
    adminLayer ++
      ZLayer.succeed(config.readerConfig) ++
      Server.defaultWithPort(serverMetadataConfig.port) ++
      ZLayer.service[Scope]
  }

  override def run: ZIO[Scope, Throwable, Nothing] = {
    (for {
      kafkaRoutes <- KafkaRoutesLive.make()
      serverProc <- Server.serve(kafkaRoutes.routes)
    } yield serverProc).provideLayer(appLayer)
  }
}
