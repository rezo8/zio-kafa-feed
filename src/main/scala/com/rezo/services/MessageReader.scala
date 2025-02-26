package com.rezo.services

import com.rezo.httpServer.Responses.Message
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import zio.ZIO
import zio.stream.ZStream

import java.time.Duration
import scala.jdk.CollectionConverters.*

object MessageReader {
  case class ReadMessageConfig(
      topic: String,
      partitions: List[Int],
      offset: Int,
      count: Int
  )

  def readMessagesForPartitions
      : ZIO[ReadMessageConfig & KafkaConsumer[String, String], Throwable, List[
        Message
      ]] = {
    for {
      consumer <- ZIO.service[KafkaConsumer[String, String]]
      config <- ZIO.service[ReadMessageConfig]
      topicPartitions = config.partitions.map(partition =>
        TopicPartition(config.topic, partition)
      )
      readRes <- ZIO
        .foreach(topicPartitions) { partition =>
          readCountMessagesFromPartition(partition, config.offset, config.count)
        }
        .map(_.flatten)
    } yield readRes
  }

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
      messages <- ZStream
        .fromIterable(recordsToProc.asScala)
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
    } yield (messages, (recordsToProc.count() == 0) || messages.size >= count)
  }
}
