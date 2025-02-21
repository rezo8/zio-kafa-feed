package com.rezo.kafka

import com.rezo.config.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{
  BytesDeserializer,
  Deserializer,
  StringDeserializer
}
import org.apache.kafka.common.utils.Bytes
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.{ZIO, ZLayer}

import java.util.Properties

object KafkaLayerFactory {
  def makeKafkaAdminClient(
      config: KafkaConsumerConfig
  ): ZLayer[Any, Throwable, AdminClient] = {
    ZLayer.scoped {
      AdminClient.make(AdminClientSettings.apply(config.bootstrapServers))
    }
  }

  def makeKafkaConsumer(
      config: KafkaConsumerConfig
  ): ZLayer[Any, Throwable, KafkaConsumer[String, String]] =
    ZLayer.scoped {
      ZIO.acquireRelease(
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

          new KafkaConsumer[String, String](
            props,
            new StringDeserializer(),
            new StringDeserializer()
          )
        }
      )(consumer => ZIO.attempt(consumer.close()).orDie)
    }
}
