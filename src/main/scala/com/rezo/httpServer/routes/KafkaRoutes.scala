package com.rezo.httpServer.routes

import com.rezo.config.{KafkaConsumerConfig, ReaderConfig}
import com.rezo.httpServer.Responses.LoadMessagesResponse
import com.rezo.kafka.KafkaConsumerFactory
import com.rezo.services.MessageReader
import io.circe.syntax.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import zio.http.*
import zio.{URIO, ZIO, ZLayer}

import scala.util.Random

class KafkaRoutes(
    consumerConfig: KafkaConsumerConfig,
    readerConfig: ReaderConfig
) extends RouteContainer {
  private val defaultCount = 10
  private val consumerPool
      : Seq[ZLayer[Any, Throwable, KafkaConsumer[String, String]]] =
    (0 to readerConfig.consumerCount).map(_ =>
      for {
        layer <- KafkaConsumerFactory.make(consumerConfig)
      } yield layer
    )

  override def routes: Routes[Any, Response] =
    Routes(
      Method.GET / "topic" / zio.http.string("topicName") / zio.http.int(
        "offset"
      ) -> handler { (topicName: String, offset: Int, req: Request) =>
        {
          handleLoadMessage(topicName, offset, req)
        }
      }
    )

  private def handleLoadMessage(
      topicName: String,
      offset: Int,
      req: Request
  ): URIO[Any, Response] = {
    // TODO load partition count using Kafka admin client.
    val count = req.url
      .queryParams("count")
      .headOption
      .fold(defaultCount)(res => res.toIntOption.getOrElse(defaultCount))
    val workingConsumers =
      Random.shuffle(consumerPool).take(readerConfig.parallelReads)

    val consumersWithPartitions = assignPartitionsToConsumers(
      workingConsumers.toList,
      consumerConfig.partitionList
    )
    // is there a better place for this to be instantiated?
    val requestMessageReader = new MessageReader(consumerConfig)
    val procs = consumersWithPartitions.map((consumerEnv, partitions) => {
      for {
        readMessages <- requestMessageReader
          .readMessagesForPartitions(
            topicName,
            partitions,
            offset,
            count
          )
          .provideLayer(consumerEnv)
      } yield readMessages
    })

    ZIO
      .collectAllPar(procs)
      .map(_.flatten.toList)
      .fold(
        error => {
          Response
            .error(Status.InternalServerError, message = error.getMessage)
        },
        readMessages => {
          Response.json(
            LoadMessagesResponse(readMessages)
              .asJson(LoadMessagesResponse.loadMessagesResponseEncoder)
              .toString
          )
        }
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

  def cleanUp(): ZIO[Any, Throwable, Unit] = {
    for {
      _ <- ZIO.logInfo("closed all consumers")
    } yield ()
  }

}
