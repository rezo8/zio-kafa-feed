package com.rezo.config

import pureconfig.*
import pureconfig.generic.derivation.*

sealed trait DerivedConfig derives ConfigReader

case class ServerConfig(serverMetadataConfig: ServerMetadataConfig)
    extends DerivedConfig
case class ServerMetadataConfig(port: Int)

// TODO rename to producerConfig
case class PublisherConfig(topicName: String, bootstrapServers: List[String])
case class IngestionJobConfig(
    publisherConfig: PublisherConfig
) extends DerivedConfig
