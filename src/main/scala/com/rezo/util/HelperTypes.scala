package com.rezo.util

import org.apache.kafka.clients.consumer.Consumer
import zio.ZPool

object HelperTypes {
  type ConsumerPool = ZPool[Throwable, Consumer[String, String]]
}
