package com.rezo.services.ingestion

import com.rezo.config.{IngestionJobConfig, ProducerConfig}
import io.circe.*
import io.circe.syntax.*
import zio.test.*
import zio.test.Assertion.{anything, fails}
import zio.{ZIO, ZLayer}

object IngestionLogicSpec extends ZIOSpecDefault {
  override def spec = suite("IngestionLogicSpec")(
    test("should successfully ingest and publish data") {
      for {
        fetcher <- ZIO.service[TestDataFetcher]
        publisher <- ZIO.service[TestDataPublisher]
        ingestionLogic <- ZIO.service[IngestionLogic]

        testData = List(
          1,
          2
        )
        jsonData = testData.map(_.asJson.noSpaces)
        _ <- fetcher.overwriteState(Right(jsonData))
        _ <- publisher.overwriteState(Right(()))

        result <- ingestionLogic.run.exit
      } yield assertCompletes
    },
    test("fails on fetch data failure") {
      for {
        fetcher <- ZIO.service[TestDataFetcher]
        ingestionLogic <- ZIO.service[IngestionLogic]

        _ <- fetcher.overwriteState(Left(new RuntimeException("Fetch error")))
        result <- ingestionLogic.run.exit
      } yield assert(result)(fails(anything))
    },
    test("fails on publish data failure") {
      for {
        fetcher <- ZIO.service[TestDataFetcher]
        publisher <- ZIO.service[TestDataPublisher]
        ingestionLogic <- ZIO.service[IngestionLogic]

        testData = List(
          1,
          2
        )
        jsonData = testData.map(_.asJson.noSpaces)
        _ <- fetcher.overwriteState(Right(jsonData))
        _ <- publisher.overwriteState(
          Left(new RuntimeException("Publish error"))
        )

        // Run ingestion
        result <- ingestionLogic.run.exit
      } yield assert(result)(fails(anything))
    }
  ).provide(
    IngestionLogicLive.layer,
    TestDataFetcher.test,
    TestDataPublisher.test,
    ZLayer.succeed(
      IngestionJobConfig(
        ProducerConfig("test-topic", List("10")),
        1,
        "/test/path"
      )
    )
  )
}
