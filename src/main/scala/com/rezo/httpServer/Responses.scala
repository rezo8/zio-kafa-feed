package com.rezo.httpServer

import com.rezo.objects.Person
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

object Responses {
  trait ServerResponse

  case class LoadPeopleResponse(people: List[Person])

  object LoadPeopleResponse {
    implicit val loadPeopleResponseEncoder: Encoder[LoadPeopleResponse] =
      deriveEncoder[LoadPeopleResponse]
  }
}
