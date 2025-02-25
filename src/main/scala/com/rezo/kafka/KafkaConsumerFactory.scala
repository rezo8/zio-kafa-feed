package com.rezo.kafka

import com.rezo.config.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import zio.{ZIO, ZLayer}

import java.util.Properties

object KafkaConsumerFactory {

  // ZLayer to create a KafkaConsumer[String, String]
  def make(
      config: KafkaConsumerConfig
  ): ZLayer[Any, Throwable, KafkaConsumer[String, String]] =
    ZLayer.scoped {
      ZIO.acquireRelease(
        ZIO.attempt {
          val props = new Properties()
          props.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            config.bootstrapServers
          )
          props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")
          props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
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
      )(consumer => ZIO.attempt(consumer.close()).orDie)
    }
}
