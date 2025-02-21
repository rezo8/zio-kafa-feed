package com.rezo.httpServer

import com.rezo.objects.Person
import io.circe.{Encoder, Json}
import io.circe._
import io.circe.parser._
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.*

object Responses {
  trait ServerResponse

  case class LoadMessagesResponse[A](data: List[Message[A]])

  object LoadMessagesResponse {
    implicit def loadMessagesResponseEncoder[A](implicit
        encoderA: Encoder[A]
    ): Encoder[LoadMessagesResponse[A]] =
      (loadMessagesResponse: LoadMessagesResponse[A]) => {
        implicit val msgDecoder: Encoder[Message[A]] =
          Message.messageEncoder(encoderA)
        Json.obj(
          ("data", loadMessagesResponse.data.asJson())
        )
      }
  }

  case class Message[A](
      key: String,
      topic: String,
      rawMessage: A,
      partition: Long,
      offset: Long
  )

  object Message {
    // Generic encoder for Message[A]
    implicit def messageEncoder[A](implicit
        encoderA: Encoder[A]
    ): Encoder[Message[A]] =
      (message: Message[A]) => {
        Json.obj(
          ("key", Json.fromString(message.key)),
          ("topic", Json.fromString(message.topic)),
          ("rawMessage", message.rawMessage.asJson),
          ("partition", Json.fromLong(message.partition)),
          ("offset", Json.fromLong(message.offset))
        )
      }
  }

  case class LoadPeopleResponse(people: List[Person])

  object LoadPeopleResponse {
    implicit val loadPeopleResponseEncoder: Encoder[LoadPeopleResponse] =
      deriveEncoder[LoadPeopleResponse]
  }
}
