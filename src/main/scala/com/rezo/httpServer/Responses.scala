package com.rezo.httpServer

import com.rezo.objects.Person
import io.circe.{Encoder, Json}
import io.circe._, io.circe.parser._
import io.circe.generic.semiauto.deriveEncoder

object Responses {
  trait ServerResponse

  case class LoadMessagesResponse(data: List[Message])

  object LoadMessagesResponse {
    implicit val loadMessagesResponseEncoder: Encoder[LoadMessagesResponse] =
      deriveEncoder[LoadMessagesResponse]
  }
  case class Message(
      key: String,
      topic: String,
      rawMessage: String,
      partition: Long,
      offset: Long
  )

  object Message {
    implicit val messageEncoder: Encoder[Message] = new Encoder[Message] {
      final def apply(a: Message): Json = {
        val jsonMsg =
          parse(a.rawMessage).getOrElse(Json.fromString(a.rawMessage))
        Json.obj(
          ("key", Json.fromString(a.key)),
          ("topic", Json.fromString(a.topic)),
          ("rawMessage", jsonMsg),
          ("partition", Json.fromLong(a.partition)),
          ("offset", Json.fromLong(a.offset))
        )
      }
    }
  }

  case class LoadPeopleResponse(people: List[Person])

  object LoadPeopleResponse {
    implicit val loadPeopleResponseEncoder: Encoder[LoadPeopleResponse] =
      deriveEncoder[LoadPeopleResponse]
  }
}
