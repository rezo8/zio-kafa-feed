package com.rezo.config

import pureconfig.*
import pureconfig.generic.derivation.*

sealed trait DerivedConfig derives ConfigReader

case class ServerConfig(
    serverMetadataConfig: ServerMetadataConfig,
    consumerConfig: KafkaConsumerConfig
) extends DerivedConfig
case class ServerMetadataConfig(port: Int)
case class KafkaConsumerConfig(
    bootstrapServers: String,
    groupId: String,
    topicName: String,
    partitionList: List[Int]
)

// TODO rename to producerConfig
case class PublisherConfig(topicName: String, bootstrapServers: List[String])
case class IngestionJobConfig(
    publisherConfig: PublisherConfig
) extends DerivedConfig
