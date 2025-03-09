package com.rezo

import com.rezo.config.{
  DerivedConfig,
  ReaderConfig,
  ServerConfig,
  ServerMetadataConfig
}
import com.rezo.exceptions.Exceptions.ConfigLoadException
import com.rezo.httpServer.routes.KafkaRoutesLive
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

//  type ConsumerPool = ZPool[Throwable, KafkaConsumer[String, String]]

//  private val consumerPoolLayer: ZLayer[Scope, Throwable, ConsumerPool] =
//    ZLayer.fromZIO {
//      ZPool.make(
//        KafkaClientFactory.makeKafkaConsumerZio.provideLayer(
//          ZLayer.succeed(config.consumerConfig)
//        ),
//        config.readerConfig.consumerCount
//      )
//    }

  val appLayer: ZLayer[
    Scope,
    Throwable,
    AdminClient & ReaderConfig & Server
  ] =
    adminLayer ++
      ZLayer.succeed(config.readerConfig) ++
      Server.defaultWithPort(serverMetadataConfig.port)

  override def run: ZIO[Scope, Throwable, Nothing] = {
    (for {
      kafkaRoutes <- KafkaRoutesLive.make()
      serverProc <- Server.serve(kafkaRoutes.routes)
    } yield { serverProc }).provideLayer(appLayer)
  }
}
