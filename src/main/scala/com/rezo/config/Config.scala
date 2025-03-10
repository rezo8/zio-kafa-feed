package com.rezo.config

import zio.*
import zio.config.magnolia.deriveConfig
import zio.config.typesafe.TypesafeConfigProvider.*

case class IngestionJobConfig(
    producerConfig: ProducerConfig,
    batchSize: Int,
    filePath: String
)

object IngestionJobConfig {
  implicit val config: Config[IngestionJobConfig] = {
    {
      Config.int("batchSize").withDefault(100) zip
        Config.string("filePath").withDefault("random-people-data.json") zip
        ProducerConfig.config.nested("producerConfig")
    }.map { case (batchSize, filePath, producerConfig) =>
      IngestionJobConfig(producerConfig, batchSize, filePath)
    }.nested("ingestionJob")
  }
}

case class ServerConfig(
    consumerConfig: KafkaConsumerConfig,
    readerConfig: ReaderConfig
)

object ServerConfig {
  implicit val config: Config[ServerConfig] = {
    {
      KafkaConsumerConfig.config.nested("consumer") zip
        ReaderConfig.config.nested("reader")
    }.map { case (consumerConfig, readerConfig) =>
      ServerConfig(consumerConfig, readerConfig)
    }.nested("server")
  }
}

case class KafkaConsumerConfig(
    bootstrapServers: List[String],
    topicName: String,
    maxPollRecords: Int
)

object KafkaConsumerConfig {
  implicit val config: Config[KafkaConsumerConfig] = {
    deriveConfig[KafkaConsumerConfig]
  }
}

case class ReaderConfig(
    consumerCount: Int,
    parallelReads: Int
)

object ReaderConfig {
  implicit val config: Config[ReaderConfig] = {
    deriveConfig[ReaderConfig]
  }
}

case class ProducerConfig(topicName: String, bootstrapServers: List[String])

object ProducerConfig {
  implicit val config: Config[ProducerConfig] = {
    deriveConfig[ProducerConfig]
  }
}
