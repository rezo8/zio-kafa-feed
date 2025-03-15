package com.rezo.services.ingestion

import zio.*
import zio.kafka.serde.Serde
import zio.kafka.testkit.{Kafka, KafkaTestUtils}
import zio.test.*
import zio.test.Assertion.*
import zio.test.TestAspect.ignore

object DataPublisherSpec extends ZIOSpecDefault {

  case class TestData(id: String, value: String)

  val testData: List[TestData] = List(
    TestData("1", "alpha"),
    TestData("2", "beta"),
    TestData("3", "gamma")
  )

  val testSerde: Serde[Any, TestData] =
    Serde.string.inmap(TestData("", _))(_.value)

  def spec = suite("DataPublisherLiveSpec")(
    test("customPublish should successfully publish records") {
      for {
        _ <- DataPublisherLive
          .customPublish(
            data = testData,
            topic = "test-topic",
            batchSize = 2,
            customSerde = testSerde,
            getId = _.id
          )
          .provideLayer(KafkaTestUtils.producer >>> DataPublisherLive.layer)
      } yield assertCompletes
    }.provideSomeShared[Scope](Kafka.embedded),
    test("customPublish should fail with a failing producer") {
      for {
        result <- DataPublisherLive
          .customPublish(
            data = testData,
            topic = "test-topic",
            batchSize = 2,
            customSerde = testSerde,
            getId = _.id
          )
          .provideLayer(KafkaTestUtils.producer >>> DataPublisherLive.layer)
          .exit
      } yield assert(result)(fails(anything))
    }.provideSomeShared[Scope](
      Kafka.embeddedWith(_ => Map("group.min.session.timeout.ms" -> "0"))
    ) @@ ignore // Find out a way to get this to fail.
  )
}
