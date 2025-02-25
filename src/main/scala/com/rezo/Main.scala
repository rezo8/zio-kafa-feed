package com.rezo

import com.rezo.config.{DerivedConfig, ServerConfig, ServerMetadataConfig}
import com.rezo.exceptions.Exceptions.ConfigLoadException
import com.rezo.httpServer.routes.KafkaRoutes
import com.rezo.kafka.KafkaLayerFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import pureconfig.ConfigSource
import zio.http.Server
import zio.kafka.admin.AdminClient
import zio.{Scope, URIO, ZIO, ZIOAppDefault, ZLayer, ZPool}

object Main extends ZIOAppDefault {

  // Load the configuration
  val config: ServerConfig = ConfigSource.default
    .at("server")
    .load[DerivedConfig]
    .getOrElse(throw new ConfigLoadException())
    .asInstanceOf[ServerConfig]

  private val serverMetadataConfig: ServerMetadataConfig =
    config.serverMetadataConfig

  // Define the consumer pool layer
  private val consumerPoolLayer = KafkaLayerFactory.makeKafkaConsumerPool(
    config.consumerConfig,
    config.readerConfig.consumerCount
  )

  private val adminLayer: ZLayer[Any, Throwable, AdminClient] =
    KafkaLayerFactory.makeKafkaAdminClient(config.consumerConfig)

  // Define the server layer
  private val serverLayer: ZLayer[Any, Throwable, Server] =
    Server.defaultWithPort(serverMetadataConfig.port)

  // Combine all layers
  private val appLayer: ZLayer[
    Any with Scope,
    Throwable,
    AdminClient & ZPool[Throwable, KafkaConsumer[String, String]] & Server
  ] =
    consumerPoolLayer ++ adminLayer ++ serverLayer

  // Define the application logic
  private def appLogic: ZIO[Scope, Throwable, Int] = {
    for {
      _ <- ZIO.logInfo("Starting the server...")
      _ <- Server
        .serve(KafkaRoutes.routes(config.readerConfig))
        .provideSomeLayer[Scope](appLayer)
    } yield 1
  }

  // Cleanup resources
  private def cleanup: URIO[Any, Int] = {
    ZIO.logInfo("Cleaning up resources...").as(1)
  }

  // Main entry point
  override def run: ZIO[Scope, Throwable, Int] = {
    appLogic.ensuring(cleanup)
  }
}
