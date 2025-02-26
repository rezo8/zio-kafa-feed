package com.rezo

import com.rezo.config.{
  DerivedConfig,
  ReaderConfig,
  ServerConfig,
  ServerMetadataConfig
}
import com.rezo.exceptions.Exceptions.ConfigLoadException
import com.rezo.httpServer.routes.KafkaRoutes
import com.rezo.kafka.KafkaClientFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import pureconfig.ConfigSource
import zio.http.Server
import zio.kafka.admin.AdminClient
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
    KafkaClientFactory.makeKafkaAdminClient(config.consumerConfig)

  type ConsumerPool = ZPool[Throwable, KafkaConsumer[String, String]]

  private val consumerPoolLayer: ZLayer[Scope, Throwable, ConsumerPool] =
    ZLayer.fromZIO {
      ZPool.make(
        KafkaClientFactory.makeKafkaConsumerZio.provideLayer(
          ZLayer.succeed(config.consumerConfig)
        ),
        config.readerConfig.consumerCount
      )
    }

  private val appLayer: ZLayer[
    Scope & Any,
    Throwable,
    AdminClient & ConsumerPool & Server & ReaderConfig
  ] =
    ZLayer.succeed(
      config.readerConfig
    ) ++ adminLayer ++ consumerPoolLayer ++ Server.defaultWithPort(
      serverMetadataConfig.port
    )

  private def startServer: ZIO[Scope, Throwable, Nothing] = {
    Server
      .serve(KafkaRoutes.routes)
      .provideSomeLayer(appLayer)
  }

  override def run: ZIO[Any, Throwable, Int] = {
    ZIO.scoped {
      for {
        _ <- ZIO.logInfo("Starting the server...")
        _ <- startServer
      } yield 1
    }
  }
}
