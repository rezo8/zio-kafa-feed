package com.rezo.services

import com.rezo.config.ReaderConfig
import com.rezo.httpServer.Responses.Message
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import zio.kafka.admin.AdminClient
import zio.stream.ZStream
import zio.{ZIO, ZLayer}

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

trait MessageReader {
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

private final case class MessageReaderLive(adminClient: AdminClient)
    extends MessageReader {

  private def createConsumer(bootstrapServers: String) = {
    val props = new Properties()
    props.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      bootstrapServers
    )
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    new KafkaConsumer[String, String](props)
  }

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
      .foreachPar(partitions)(partition =>
        readCountMessagesFromPartition(
          TopicPartition(topic, partition),
          offset,
          count
        )
      )
      .map(_.flatten)
  }

  private def readCountMessagesFromPartition(
      partition: TopicPartition,
      offset: Int,
      count: Int,
      accumulatedMessages: Seq[Message] = Seq.empty
  ): ZIO[Any, Throwable, Seq[Message]] = {
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
    val consumer = createConsumer("localhost:29092")

    for {
      // Log before assigning the partition
      _ <- ZIO.logDebug(s"Attempting to assign partition: $partition")
      _ <- ZIO.attempt(consumer.assign(List(partition).asJava))

      // Log assigned partitions after assignment
      assignedPartitions <- ZIO.attempt(consumer.assignment())
      _ <- ZIO.logDebug(s"Assigned partitions: $assignedPartitions")
      _ <- ZIO.logDebug(s"Seeking partition: $partition to offset: $offset")
      _ <- ZIO.attempt(consumer.seek(partition, offset))
      position <- ZIO.attempt(consumer.position(partition))
      _ <- ZIO.logDebug(s"Consumer position for $partition: $position")

      // Attempt to poll and log result
      recordsToProc <- ZIO
        .attempt(consumer.poll(Duration.ofMillis(5000)))
        .tapError(e => {
          ZIO.logError(s"Error polling Kafka: ${e.getMessage}")
        })
      _ <- ZIO.logDebug(
        s"Polling complete. Read ${recordsToProc.count()} messages from partition $partition at offset $offset"
      )
      _ <- ZIO.logDebug(
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

object MessageReaderLive {
  val layer: ZLayer[
    AdminClient,
    Throwable,
    MessageReader
  ] =
    ZLayer.fromFunction(MessageReaderLive(_))

  def readMessages(
      readerConfig: ReaderConfig,
      topic: String,
      offset: Int,
      count: Int
  ): ZIO[MessageReader, Throwable, Seq[Message]] =
    ZIO.serviceWithZIO(_.readMessages(readerConfig, topic, offset, count))

}
