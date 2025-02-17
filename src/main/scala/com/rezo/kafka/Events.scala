package com.rezo.kafka

import com.rezo.objects.Person
import zio.ZIO
import zio.json.*
import zio.kafka.serde.Serde

import java.time.Instant
import java.util.UUID

class Events

case class PersonEvent(
    eventId: UUID = UUID.randomUUID(),
    eventTime: Instant = Instant.now(),
    person: Person
)
object PersonEvent {
  implicit val encoder: JsonEncoder[PersonEvent] =
    DeriveJsonEncoder.gen[PersonEvent]
  implicit val decoder: JsonDecoder[PersonEvent] =
    DeriveJsonDecoder.gen[PersonEvent]

  val serde: Serde[Any, PersonEvent] = Serde.string.inmapZIO { str =>
    ZIO
      .fromEither(str.fromJson[PersonEvent])
      .mapError(e =>
        new RuntimeException(s"Failed to deserialize PersonEvent: $e")
      )
  } { PersonEvent =>
    ZIO.succeed(PersonEvent.toJson)
  }
}
