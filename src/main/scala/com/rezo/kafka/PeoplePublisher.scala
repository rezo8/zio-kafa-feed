package com.rezo.kafka

import com.rezo.config.PublisherConfig
import com.rezo.objects.Person

class PeoplePublisher(config: PublisherConfig) {

  def produce(
      person: Person
  ) = {
    val createUserEvent = PersonEvent(person = person)
    Producer
      .produce[Any, UUID, PersonEvent](
        config.topicName,
        user.userId,
        createUserEvent,
        keySerializer = Serde.uuid,
        valueSerializer = CreateUserEvent.serde
      )
      .mapError(exception => {
        PublishError(exception)
      })
  }
}
