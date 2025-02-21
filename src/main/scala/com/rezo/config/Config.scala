package com.rezo.config

import pureconfig.*
import pureconfig.generic.derivation.*

sealed trait DerivedConfig derives ConfigReader

case class ServerConfig(
    serverMetadataConfig: ServerMetadataConfig,
    readerConfig: ReaderConfig,
    consumerConfig: KafkaConsumerConfig
) extends DerivedConfig
case class ServerMetadataConfig(port: Int)
case class KafkaConsumerConfig(
    bootstrapServers: List[String],
    topicName: String,
    maxPollRecords: Int
)

case class ReaderConfig(
    consumerCount: Int,
    parallelReads: Int
)
// TODO rename to producerConfig
case class PublisherConfig(topicName: String, bootstrapServers: List[String])
case class IngestionJobConfig(
    publisherConfig: PublisherConfig,
    batchSize: Int
) extends DerivedConfig
