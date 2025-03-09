package com.rezo

import com.rezo.config.{DerivedConfig, IngestionJobConfig}
import com.rezo.exceptions.Exceptions.ConfigLoadException
import com.rezo.services.ingestion.*
import pureconfig.ConfigSource
import zio.*
import zio.kafka.producer.{Producer, ProducerSettings}

object IngestionJob extends ZIOAppDefault {
  // TODO maybe there's a ZIO-y config loading.
  val config: IngestionJobConfig = ConfigSource.default
    .at("ingestion-job")
    .load[DerivedConfig]
    .getOrElse(throw new ConfigLoadException())
    .asInstanceOf[IngestionJobConfig]

  private val filePath = "random-people-data.json"

  private val producerLayer: ZLayer[Any, Throwable, Producer] =
    ZLayer.scoped(
      Producer.make(ProducerSettings(config.publisherConfig.bootstrapServers))
    )

  override def run: ZIO[Any, Throwable, Unit] = {
    IngestionLogicLive.run.provide(
      IngestionLogicLive.layer,
      DataFetcherLive.layer,
      DataPublisherLive.layer,
      ZLayer.succeed(filePath),
      producerLayer
    )
  }
}
