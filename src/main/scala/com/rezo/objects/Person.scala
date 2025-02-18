package com.rezo.objects

import io.circe.*
import io.circe.generic.semiauto.*
import io.circe.parser.*
import io.circe.syntax.*
import zio.*
import zio.kafka.serde.*

case class CtRoot(
    ctRoot: List[Json]
)

object CtRoot {
  implicit val rawCtListDecoder: Decoder[CtRoot] = deriveDecoder[CtRoot]
}

case class Person(
    _id: String,
    name: String,
    dob: String,
    address: Address,
    telephone: String, // TODO validation
    pets: List[String],
    score: Double,
    email: String,
    description: String,
    verified: Boolean,
    salary: Long
)

object Person {
  implicit val personDecoder: Decoder[Person] = deriveDecoder[Person]
  implicit val personEncoder: Encoder[Person] = deriveEncoder[Person]

  implicit val serde: Serde[Any, Person] =
    Serde.string.inmapZIO(json => ZIO.fromEither(decode[Person](json)))(
      person => ZIO.succeed(person.asJson.noSpaces)
    )

}

case class Address(
    street: String,
    town: String,
    postcode: String
)

object Address {

  implicit val addressDecoder: Decoder[Address] = (c: HCursor) =>
    for {
      street <- c.downField("street").as[String]
      town <- c.downField("town").as[String]
      postcode <- c.downField("postode").as[String]
    } yield {
      new Address(street, town, postcode)
    }

  implicit val addressEncoder: Encoder[Address] = deriveEncoder[Address]
}
