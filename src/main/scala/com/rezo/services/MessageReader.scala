package com.rezo.services

import com.rezo.config.KafkaConsumerConfig
import com.rezo.httpServer.Responses.Message
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import zio.ZIO
import zio.stream.ZStream

import java.time.Duration
import scala.jdk.CollectionConverters.*

class MessageReader(config: KafkaConsumerConfig) {
  def readMessagesForPartitions(
      topic: String,
      partitions: List[Int],
      offset: Int,
      count: Int
  ): ZIO[KafkaConsumer[String, String], Throwable, List[Message]] = {
    val topicPartitions =
      partitions.map(partition => TopicPartition(topic, partition))
    for {
      consumer <- ZIO.service[KafkaConsumer[String, String]]
      readRes <- ZIO
        .foreach(topicPartitions) { partition =>
          readCountMessagesFromPartition(partition, offset, count)
        }
        .map(_.flatten)
    } yield readRes
  }

  // TODO need to make sure the read recurses until the records is empty or the list size is count.
  private def readCountMessagesFromPartition(
      partition: TopicPartition,
      offset: Int,
      count: Int,
      accumulatedMessages: List[Message] = List.empty
  ): ZIO[KafkaConsumer[String, String], Throwable, List[Message]] = {
    consumeOffPartition(partition, offset, count).flatMap {
      case (messages, stopReading) =>
        val allMessages = accumulatedMessages ++ messages
        if (stopReading || allMessages.size >= count) {
          ZIO.succeed(allMessages)
        } else {
          readCountMessagesFromPartition(
            partition,
            offset + messages.size,
            count - allMessages.size,
            allMessages
          )
        }
    }
  }

  private def consumeOffPartition(
      partition: TopicPartition,
      offset: Int,
      count: Int
  ) = {
    for {
      consumer <- ZIO.service[KafkaConsumer[String, String]]
      _ <- ZIO.attempt(consumer.assign(List(partition).asJava))
      _ <- ZIO.attempt(consumer.seek(partition, offset))
      recordsToProc <- ZIO.attempt(consumer.poll(Duration.ofMillis(1000)))
      _ <- ZIO.logInfo(
        s"Reading ${recordsToProc.count()} messages off of partition ${partition} and offset ${offset}"
      )
      stopReading = recordsToProc.count() == 0
      messages <- ZStream
        .fromIterable(recordsToProc.asScala)
        .take(count) // figure out to remove chunk or take here.
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
    } yield (messages, (recordsToProc.count() == 0) || messages.size >= count)
  }
}
