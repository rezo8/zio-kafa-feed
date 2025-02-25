package com.rezo.httpServer.routes

import com.rezo.config.ReaderConfig
import com.rezo.httpServer.Responses.LoadMessagesResponse
import com.rezo.services.MessageReader
import io.circe.syntax.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import zio.http.*
import zio.kafka.admin.AdminClient
import zio.{Scope, ZIO, ZLayer, ZPool}

object KafkaRoutes {

  // Define the default count for messages to fetch
  private val defaultCount = 10

  // Routes definition
  def routes(readerConfig: ReaderConfig): Routes[
    Scope & ZPool[Throwable, KafkaConsumer[String, String]] & AdminClient,
    Response
  ] =
    Routes(
      Method.GET / "topic" / string("topicName") / int("offset") -> handler {
        (topicName: String, offset: Int, req: Request) =>
          handleLoadMessage(topicName, offset, req, readerConfig)
      }
    )

  // Handler for loading messages
  private def handleLoadMessage(
      topicName: String,
      offset: Int,
      req: Request,
      readerConfig: ReaderConfig
  ): ZIO[
    Scope & ZPool[Throwable, KafkaConsumer[String, String]] & AdminClient,
    Nothing,
    Response
  ] = {
    val count = req.url
      .queryParams("count")
      .headOption
      .flatMap(_.toIntOption)
      .getOrElse(defaultCount)

    val requestMessageReader = new MessageReader

    (for {
      adminClient <- ZIO.service[AdminClient]
      pool <- ZIO.service[ZPool[Throwable, KafkaConsumer[String, String]]]
      partitions <- adminClient
        .describeTopics(List(topicName))
        .map(_.get(topicName).map(_.partitions.map(_.partition)))
        .someOrFail(new RuntimeException(s"Topic $topicName not found"))

      consumers <- ZIO.foreach(partitions) { partition =>
        pool.get.map(consumer => (consumer, partition))
      }
      readMessages <- ZIO
        .collectAllPar(
          consumers.map { case (consumer, partition) =>
            requestMessageReader
              .readMessagesForPartitions(
                topicName,
                List(partition),
                offset,
                count
              )
              .provide(ZLayer.succeed(consumer))
          }
        )
        .map(_.flatten.toList)
      response = Response.json(
        LoadMessagesResponse(readMessages).asJson.toString
      )
    } yield response)
      .catchAll(error =>
        ZIO.succeed(
          Response.error(Status.InternalServerError, error.getMessage)
        )
      )
  }

  private def assignPartitionsToConsumers[A, B](
      consumers: List[A],
      partitions: List[B]
  ): Map[A, List[B]] = {
    val numConsumers = consumers.size
    val numPartitions = partitions.size

    val assignmentMap =
      consumers.map(consumer => consumer -> List.empty[B]).toMap

    partitions.zipWithIndex.foldLeft(assignmentMap) {
      case (acc, (partition, index)) =>
        val consumerIndex =
          index % numConsumers
        val consumer = consumers(consumerIndex)
        val updatedPartitions =
          acc(consumer) :+ partition
        acc + (consumer -> updatedPartitions)
    }
  }
}
