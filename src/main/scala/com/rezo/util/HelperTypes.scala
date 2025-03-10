package com.rezo.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import zio.ZPool

object HelperTypes {
  type ConsumerPool = ZPool[Throwable, KafkaConsumer[String, String]]
}
