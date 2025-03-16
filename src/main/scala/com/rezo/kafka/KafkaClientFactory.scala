package com.rezo.kafka

import com.rezo.config.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.{
  Consumer,
  ConsumerConfig,
  KafkaConsumer
}
import org.apache.kafka.common.serialization.StringDeserializer
import zio.ZIO

import java.util.Properties

object KafkaClientFactory {
  def makeKafkaConsumer(
      config: KafkaConsumerConfig
  ): ZIO[Any, Throwable, Consumer[String, String]] = {
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
}
