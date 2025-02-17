package com.rezo

import com.rezo.config.{DerivedConfig, ServerConfig, ServerMetadataConfig}
import com.rezo.httpServer.BaseServer
import com.rezo.httpServer.routes.KafkaRoutes
import exceptions.Exceptions.ConfigLoadException
import pureconfig.ConfigSource
import zio.{ZIO, ZIOAppDefault}

object Main extends ZIOAppDefault with BaseServer { env =>

  val config: ServerConfig = ConfigSource.default
    .at("server")
    .load[DerivedConfig]
    .getOrElse(throw new ConfigLoadException())
    .asInstanceOf[ServerConfig]

  override val serverMetadataConfig: ServerMetadataConfig =
    config.serverMetadataConfig
  override val kafkaRoutes: KafkaRoutes = new KafkaRoutes

  private def appLogic: ZIO[Any, Throwable, Nothing] = {
    for {
      serverProc <- startServer
    } yield serverProc
  }

  private def cleanup = {
    // Fortunately ZIO Http Server comes with graceful shutdown built in: https://github.com/zio/zio-http/pull/2099/files
    println("shutting down")
    ZIO.unit
  }

  override def run: ZIO[Any, Throwable, Int] = {
    appLogic.ensuring(cleanup)
  }
}
