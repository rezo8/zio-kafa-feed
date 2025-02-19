//package com.rezo.services
//
//import com.rezo.exceptions.Exceptions.RezoException
//import com.rezo.objects.Person
//import org.apache.kafka.clients.consumer.ConsumerRecords
//import org.apache.kafka.common.TopicPartition
//import zio.*
//
//import java.util
//import java.util.Collection
//
//trait Request
//
//trait KafkaConsumerConfig {
//  val topic: String
//  val offset: Long
//}
//
//trait KafkaConsumer {
//  def assign(
//      partitions: Seq[TopicPartition]
//  ): ZIO[KafkaConsumerConfig, RezoException, Unit]
//
//  def seek(
//      partition: TopicPartition,
//      offset: Long
//  ): ZIO[KafkaConsumerConfig, RezoException, Unit]
//
//  def poll(
//      timeout: Duration
//  ): ZIO[Any, RezoException, ConsumerRecords[String, String]]
//}
//
//trait GetMessages {
//  def run(
//      topic: String,
//      partition: Int,
//      offset: Int,
//      count: Int
//  ): ZIO[Any, Throwable, List[Person]]
//}
//
//abstract class MessageReaderZIO extends G{
//
//  lazy val workflow: ZIO[Request & Scope, Throwable, Unit]
//}
