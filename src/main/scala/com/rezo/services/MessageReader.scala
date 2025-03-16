package com.rezo.services

import com.rezo.config.ReaderConfig
import com.rezo.httpServer.Responses.Message
import com.rezo.util.HelperTypes.ConsumerPool
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import zio.kafka.admin.AdminClient
import zio.stream.ZStream
import zio.{ZIO, ZLayer}

import java.time.Duration
import scala.jdk.CollectionConverters.*

trait MessageReader {
  def readMessages(
      readerConfig: ReaderConfig,
      topicName: String,
      offset: Int,
      count: Int
  ): ZIO[Any, Throwable, Seq[Message]]
}

private final case class MessageReaderLive(
    adminClient: AdminClient,
    consumerPool: ConsumerPool
) extends MessageReader {

  def readMessages(
      readerConfig: ReaderConfig,
      topicName: String,
      offset: Int,
      count: Int
  ): ZIO[Any, Throwable, Seq[Message]] = {
    for {
      topicDescription <- adminClient
        .describeTopics(List(topicName))
        .map(_.get(topicName))
        .someOrFail(new RuntimeException(s"Topic ${topicName} not found"))

      res <- processForAllPartitionsZio(
        topicName,
        topicDescription.partitions.map(_.partition),
        offset,
        count
      )
    } yield res
  }

  private def processForAllPartitionsZio(
      topic: String,
      partitions: List[Int],
      offset: Int,
      count: Int
  ): ZIO[Any, Throwable, Seq[Message]] = {
    ZIO
      .foreach(partitions)(
        partition => // Use `foreach` instead of `foreachPar` for thread safety
          ZIO.scoped {
            consumerPool.get.flatMap { consumer =>
              readCountMessagesFromPartition(
                consumer,
                TopicPartition(topic, partition),
                offset,
                count
              )
            }
          }
      )
      .map(_.flatten)
  }

  private def readCountMessagesFromPartition(
      consumer: Consumer[String, String],
      partition: TopicPartition,
      offset: Int,
      count: Int,
      accumulatedMessages: Seq[Message] = Seq.empty
  ): ZIO[Any, Throwable, Seq[Message]] = {
    consumeOffPartition(consumer, partition, offset, count).flatMap {
      case (messages, stopReading) =>
        val allMessages = accumulatedMessages ++ messages
        if (stopReading || allMessages.size >= count) {
          ZIO.succeed(allMessages)
        } else {
          readCountMessagesFromPartition(
            consumer,
            partition,
            offset + messages.size,
            count,
            allMessages
          )
        }
    }
  }

  private def consumeOffPartition(
      consumer: Consumer[String, String],
      partition: TopicPartition,
      offset: Int,
      count: Int
  ) = {
    for {
      _ <- ZIO.logInfo(s"Assigning partition: $partition")
      _ <- ZIO.attemptBlocking(consumer.assign(List(partition).asJava))
      _ <- ZIO.logInfo(s"Seeking partition: $partition to offset: $offset")
      _ <- ZIO.attemptBlocking(consumer.seek(partition, offset))
      position <- ZIO.attemptBlocking(consumer.position(partition))
      _ <- ZIO.logInfo(s"Consumer position for $partition: $position")

      recordsToProc <- ZIO
        .attemptBlocking(consumer.poll(Duration.ofMillis(1000)))
        .tapError(e => ZIO.logError(s"Error polling Kafka: ${e.getMessage}"))
      _ <- ZIO.logInfo(
        s"Polling complete. Read ${recordsToProc.count()} messages from partition $partition at offset $offset"
      )

      messages <- ZStream
        .fromIterable(recordsToProc.asScala)
        .take(count)
        .map(record =>
          Message(
            key = record.key(),
            topic = record.topic(),
            rawMessage = record.value(),
            partition = record.partition(),
            offset = record.offset()
          )
        )
        .runFold(List.empty[Message]) { (acc, result) => acc :+ result }
    } yield (messages, (recordsToProc.count() == 0) || messages.size >= count)
  }
}

object MessageReaderLive {
  val layer: ZLayer[
    AdminClient & ConsumerPool,
    Throwable,
    MessageReader
  ] =
    ZLayer.fromFunction(MessageReaderLive(_, _))

  def readMessages(
      readerConfig: ReaderConfig,
      topic: String,
      offset: Int,
      count: Int
  ): ZIO[MessageReader, Throwable, Seq[Message]] =
    ZIO.serviceWithZIO(_.readMessages(readerConfig, topic, offset, count))
}
