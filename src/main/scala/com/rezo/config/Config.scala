package com.rezo.config

import pureconfig.*
import pureconfig.generic.derivation.*

sealed trait DerivedConfig derives ConfigReader

case class ServerConfig(serverMetadataConfig: ServerMetadataConfig)
    extends DerivedConfig
case class ServerMetadataConfig(port: Int)
