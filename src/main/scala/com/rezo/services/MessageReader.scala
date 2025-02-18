package com.rezo.services

import com.rezo.config.KafkaConsumerConfig
import com.rezo.objects.Person
import io.circe.parser.*
import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  ConsumerRecords,
  KafkaConsumer
}
import org.apache.kafka.common.TopicPartition
import zio.ZIO

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

class MessageReader(config: KafkaConsumerConfig) {
  private def createConsumer(
      bootstrapServers: String,
      groupId: String
  ) = {
    val props = new Properties()
    props.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      bootstrapServers
    )
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
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

  def processForAllPartitionsZio(
      topic: String,
      partitions: List[Int],
      offset: Int,
      count: Int
  ): ZIO[Any, Throwable, List[Person]] = {
    ZIO
      .foreachPar(partitions)(partition =>
        processZio(topic, partition, offset, count)
      )
      .map(_.flatten)
  }

  private def processZio(
      topic: String,
      partition: Int,
      offset: Int,
      count: Int
  ): ZIO[Any, Throwable, List[Person]] = {
    val consumer = createConsumer(
      config.bootstrapServers,
      config.groupId
    )
    val topicPartition = TopicPartition(topic, partition)
    for {
      _ <- ZIO.attempt(
        consumer.assign(List(topicPartition).asJava)
      )
      _ <- ZIO.attempt(
        consumer.seek(topicPartition, offset)
      )

      personList <- ZIO.attempt {
        var persons: List[Person] = List()
        var messagesProcessed = 0

        while (messagesProcessed < count) {
          val records: ConsumerRecords[String, String] =
            consumer.poll(Duration.ofMillis(1000))
          if (records.isEmpty) {
            messagesProcessed = count
          }

          // Process the records and decode them
          for (record <- records.asScala) {
            if (messagesProcessed < count) {
              decode[Person](record.value()) match {
                case Right(person) =>
                  persons = persons :+ person
                  messagesProcessed += 1
                case Left(error) =>
                  ZIO
                    .logError(
                      s"Error parsing message: ${record.value()}, error: $error"
                    )
                    .ignore
              }
            }
          }
        }
        persons
      }
    } yield personList
  }

//  def processForAllPartitions(
//      topic: String,
//      partitions: List[Int],
//      offset: Int,
//      count: Int
//  ): List[Person] = {
//    partitions.flatMap(process(topic, _, offset, count))
//  }
//  // The ZIO version of the process function that returns a ZIO of List[Person]
//  private def process(
//      topic: String,
//      partition: Int,
//      offset: Int,
//      count: Int
//  ): List[Person] = {
//    val consumer = createConsumer(config.bootstrapServers, config.groupId)
//    val topicPartition = new TopicPartition(topic, partition)
//    consumer.assign(List(topicPartition).asJava)
//    consumer.seek(topicPartition, offset)
//
//    var personList: List[Person] = List()
//    var messagesProcessed = 0
//    try {
//      while (messagesProcessed < count) {
//        val records: ConsumerRecords[String, String] =
//          consumer.poll(Duration.ofMillis(1000))
//        if (records.isEmpty) {
//          return personList
//        }
//        for (record <- records.asScala) {
//          if (messagesProcessed < count) {
//            decode[Person](record.value()) match {
//              case Right(person) =>
//                personList = personList :+ person
//                messagesProcessed += 1
//              case Left(error) =>
//                println(
//                  s"Error parsing message: ${record.value()}, error: $error"
//                )
//            }
//          }
//        }
//      }
//    } finally {
//      consumer.close()
//    }
//    personList
//  }
}
