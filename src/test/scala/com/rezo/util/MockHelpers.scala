package com.rezo.util

import com.rezo.util.HelperTypes.ConsumerPool
import org.apache.kafka.clients.consumer.{
  Consumer,
  ConsumerRecord,
  MockConsumer,
  OffsetResetStrategy
}
import org.apache.kafka.common.TopicPartition
import zio.kafka.admin.AdminClient
import zio.{Scope, ZIO, ZPool}

import scala.jdk.CollectionConverters.*

object MockHelpers {
  private def mockKafkaConsumer(
      messages: List[ConsumerRecord[String, String]]
  ): Consumer[String, String] = {
    val consumer =
      new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)
    val partition = new TopicPartition("test-topic", 0)
    consumer.assign(List(partition).asJava)
    consumer.seek(partition, 0)
    messages.foreach(consumer.addRecord)
    consumer
  }

  def mockConsumerPool(
      messages: List[ConsumerRecord[String, String]]
  ): ZIO[Scope, Throwable, ConsumerPool] = {
    ZPool.make(ZIO.attempt(mockKafkaConsumer(messages)), 5)
  }

  val createTestTopic: ZIO[AdminClient, Throwable, Unit] = {
    for {
      adminClient <- ZIO.service[AdminClient]
      _ <- adminClient.createTopic(
        AdminClient.NewTopic(
          "test-topic",
          1,
          1
        )
      )
    } yield ()
  }
}
