package com.rezo.services

import com.rezo.config.ReaderConfig
import com.rezo.httpServer.Responses.Message
import com.rezo.util.HelperTypes.ConsumerPool
import com.rezo.util.MockHelpers.{createTestTopic, mockConsumerPool}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import zio.kafka.admin.AdminClient
import zio.kafka.testkit.{Kafka, KafkaTestUtils}
import zio.test.*
import zio.test.Assertion.*
import zio.{Scope, ZIO, ZLayer}

object MessageReaderSpec extends ZIOSpecDefault {

  val testMessages: List[ConsumerRecord[String, String]] = List(
    new ConsumerRecord("test-topic", 0, 0, "key1", "value1"),
    new ConsumerRecord("test-topic", 0, 1, "key2", "value2"),
    new ConsumerRecord("test-topic", 0, 2, "key3", "value3")
  )

  val testReaderConfig: ReaderConfig = ReaderConfig(
    consumerCount = 1,
    parallelReads = 1
  )

  override def spec: Spec[Scope, Throwable] =
    (suite("MessageReaderLiveSpec")(
      test("readMessages should return messages from Kafka") {
        for {
          adminClient <- ZIO.service[AdminClient]
          consumerPool <- mockConsumerPool(testMessages)
          result <- MessageReaderLive
            .readMessages(
              testReaderConfig,
              "test-topic",
              offset = 0,
              count = 2
            )
            .provide(
              ZLayer.succeed(adminClient),
              ZLayer.succeed(consumerPool),
              MessageReaderLive.layer
            )
        } yield assert(result)(hasSize(equalTo(2)))
      },
      test(
        "readMessages should return all messages available from Kafka if searching for more than available."
      ) {
        for {
          adminClient <- ZIO.service[AdminClient]
          consumerPool <- mockConsumerPool(testMessages)
          result <- MessageReaderLive
            .readMessages(
              testReaderConfig,
              "test-topic",
              offset = 0,
              count = 10
            )
            .provide(
              ZLayer.succeed(adminClient),
              ZLayer.succeed(consumerPool),
              MessageReaderLive.layer
            )
        } yield assert(result)(hasSize(equalTo(3)))
      },
      test("readMessages should fail if topic does not exist") {
        for {
          adminClient <- ZIO.service[AdminClient]
          consumerPool <- mockConsumerPool(testMessages)
          result <- MessageReaderLive
            .readMessages(
              testReaderConfig,
              "non-existent-topic",
              offset = 0,
              count = 2
            )
            .provide(
              ZLayer.succeed(adminClient),
              ZLayer.succeed(consumerPool),
              MessageReaderLive.layer
            )
            .either
        } yield assert(result)(
          isLeft(
            hasMessage(
              equalTo("This server does not host this topic-partition.")
            )
          )
        )
      },
      test("readMessages should handle empty partitions") {
        for {
          adminClient <- ZIO.service[AdminClient]
          consumerPool <- mockConsumerPool(List.empty)
          result <- MessageReaderLive
            .readMessages(
              testReaderConfig,
              "test-topic",
              offset = 10,
              count = 2
            )
            .provide(
              ZLayer.succeed(adminClient),
              ZLayer.succeed(consumerPool),
              MessageReaderLive.layer
            )
        } yield assert(result)(isEmpty)
      },
      test("readMessages should handle offsets beyond available messages") {
        for {
          adminClient <- ZIO.service[AdminClient]
          consumerPool <- mockConsumerPool(testMessages)
          result <- MessageReaderLive
            .readMessages(
              testReaderConfig,
              "test-topic",
              offset = 5,
              count = 2
            )
            .provide(
              ZLayer.succeed(adminClient),
              ZLayer.succeed(consumerPool),
              MessageReaderLive.layer
            )
        } yield assert(result)(isEmpty)
      }
    ) @@ TestAspect
      .beforeAll(createTestTopic))
      .provideSomeShared[Scope](
        Kafka.embedded >>> ZLayer.apply(KafkaTestUtils.makeAdminClient)
      )
}
