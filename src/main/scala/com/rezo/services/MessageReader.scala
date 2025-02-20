package com.rezo.services

import com.rezo.config.KafkaConsumerConfig
import com.rezo.httpServer.Responses.Message
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import zio.ZIO
import zio.stream.ZStream

import java.time.Duration
import scala.jdk.CollectionConverters.*

class MessageReader(config: KafkaConsumerConfig) {

  def processForAllPartitionsZio(
      topic: String,
      partitions: List[Int],
      offset: Int,
      count: Int
  ): ZIO[KafkaConsumer[String, String], Throwable, List[Message]] = {
    val topicPartitions =
      partitions.map(partition => TopicPartition(topic, partition))
    ZIO
      .foreach(topicPartitions) { partition =>
        processForPartition(partition, offset, count)
      }
      .map(_.flatten)
  }

  private def processForPartition(
      partition: TopicPartition,
      offset: Int,
      count: Int
  ): ZIO[KafkaConsumer[String, String], Throwable, List[Message]] = {
    {
      for {
        consumer <- ZIO.service[KafkaConsumer[String, String]]
        _ <- ZIO.attempt(consumer.assign(List(partition).asJava))
        _ <- ZIO.attempt(consumer.seek(partition, offset))
        recordsToProc <- ZIO.attempt(consumer.poll(Duration.ofMillis(1000)))
        messages <- ZStream
          .fromIterable(recordsToProc.asScala, chunkSize = count)
          .take(count)
          .map(record => {
            Message(
              key = record.key(),
              topic = record.topic(),
              rawMessage = record.value(),
              partition = record.partition(),
              offset = record.offset()
            )
          })
          .runFold(List.empty[Message]) { (acc, result) => acc :+ result }

      } yield messages
    }
  }
}
