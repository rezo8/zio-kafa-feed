package com.rezo.kafka

import com.rezo.config.{KafkaConsumerConfig, ReaderConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.{Scope, ZEnvironment, ZIO, ZLayer, ZPool}

import java.util.Properties
import scala.collection.immutable.AbstractSeq

object KafkaLayerFactory {
  type ConsumerPool = AbstractSeq[ZEnvironment[KafkaConsumer[String, String]]]

  def makeKafkaAdminClient(
      config: KafkaConsumerConfig
  ): ZLayer[Any, Throwable, AdminClient] = {
    ZLayer.scoped {
      AdminClient.make(AdminClientSettings.apply(config.bootstrapServers))
    }
  }

  def makeKafkaConsumerPool(
      config: KafkaConsumerConfig,
      count: Int
  ): ZLayer[
    Scope,
    Nothing,
    ZPool[Throwable, KafkaConsumer[String, String]]
  ] = ZLayer.scoped { ZPool.make(makeKafkaConsumer(config), count) }

  private def makeKafkaConsumer(
      config: KafkaConsumerConfig
  ) =
    ZIO.attempt {
      val props = new Properties()
      props.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.bootstrapServers.mkString(",")
      )
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
      props.put(
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        config.maxPollRecords
      )
      props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        classOf[StringDeserializer]
      )
      props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        classOf[StringDeserializer]
      )
      new KafkaConsumer[String, String](props)
    }
}
