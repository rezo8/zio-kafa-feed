package com.rezo.services

import com.rezo.config.ReaderConfig
import com.rezo.httpServer.Responses.Message
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import zio.kafka.admin.AdminClient
import zio.stream.ZStream
import zio.{ZIO, ZLayer, ZPool}

import java.time.Duration
import scala.jdk.CollectionConverters.*

trait MessageReaderTwo {
  def readMessages(
      readerConfig: ReaderConfig,
      topicName: String,
      offset: Int,
      count: Int
  ): ZIO[Any, Throwable, Seq[Message]]
}

private case class MessageReaderConfig(
    topic: String,
    partitions: List[Int],
    offset: Int,
    count: Int
)

type ConsumerPoolTwo = ZPool[Throwable, KafkaConsumer[String, String]]

private final case class MessageReaderTwoLive(
    adminClient: AdminClient,
    consumerPool: ConsumerPoolTwo
) extends MessageReaderTwo {
  def readMessages(
      readerConfig: ReaderConfig,
      topicName: String,
      offset: Int,
      count: Int
  ): ZIO[Any, Throwable, Seq[Message]] = {
    for {
      partitions <- adminClient
        .describeTopics(List(topicName))
        .map(_.get(topicName).map(_.partitions.map(_.partition)))
        .someOrFail(
          new RuntimeException(s"Topic $topicName not found")
        ) // Better exception here.
      partitionGroups = partitions
        .grouped(
          (partitions.size + readerConfig.parallelReads - 1) / readerConfig.parallelReads
        )
        .toSeq
      allMessages <- ZIO.foreachPar(partitionGroups) { partitionGroup =>
        {
          ZIO.scoped {
            for {
              consumer <- ZIO.acquireRelease(consumerPool.get)(x =>
                consumerPool.invalidate(x)
              ) // Ensuring that consumerPool invalidates it once it's done (this happens for free I think but just to be explicit)
              messageReaderConfig = MessageReaderConfig(
                topicName,
                partitionGroup,
                offset,
                count
              )
              readMessages <- readMessagesForPartitions(
                messageReaderConfig,
                consumer
              )
            } yield readMessages
          }
        }
      }
    } yield allMessages.flatten
  }

  private def readMessagesForPartitions(
      messageReaderConfig: MessageReaderConfig,
      kafkaConsumer: KafkaConsumer[String, String]
  ): ZIO[Any, Throwable, List[Message]] = {
    val topicPartitions = messageReaderConfig.partitions.map(partition =>
      TopicPartition(messageReaderConfig.topic, partition)
    )
    for {
      readRes <- ZIO
        .foreach(topicPartitions) { partition =>
          readCountMessagesFromPartition(
            partition,
            messageReaderConfig.offset,
            messageReaderConfig.count
          ).provideLayer(ZLayer.succeed(kafkaConsumer))
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

object MessageReaderTwoLive {
  val layer
      : ZLayer[AdminClient & ConsumerPoolTwo, Throwable, MessageReaderTwo] =
    ZLayer.fromFunction(MessageReaderTwoLive(_, _))

  def readMessages(
      readerConfig: ReaderConfig,
      topic: String,
      offset: Int,
      count: Int
  ): ZIO[MessageReaderTwo, Throwable, Seq[Message]] =
    ZIO.serviceWithZIO(_.readMessages(readerConfig, topic, offset, count))

}
