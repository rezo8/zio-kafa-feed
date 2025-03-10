package com.rezo

import com.rezo.config.IngestionJobConfig
import com.rezo.services.ingestion.*
import zio.config.typesafe.TypesafeConfigProvider.*
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.{config, *}

object IngestionJob extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.setConfigProvider(fromResourcePath())

  private val configLayer: ZLayer[Any, Throwable, IngestionJobConfig] =
    ZLayer.fromZIO(ZIO.config[IngestionJobConfig].orDie)

  private val producerLayer: ZLayer[IngestionJobConfig, Throwable, Producer] =
    ZLayer.scoped {
      for {
        config <- ZIO.service[IngestionJobConfig]
        producer <- Producer.make(
          ProducerSettings(config.producerConfig.bootstrapServers)
        )
      } yield producer
    }

  override def run: ZIO[Any, Throwable, Unit] = {
    for {
      config <- ZIO.config[IngestionJobConfig]
      _ <- ZIO.logInfo(s"Loaded configuration: $config")
      _ <- IngestionLogicLive.run.provide(
        IngestionLogicLive.layer,
        DataFetcherLive.layer,
        DataPublisherLive.layer,
        configLayer >>> producerLayer,
        configLayer
      )
    } yield ()
  }
}
