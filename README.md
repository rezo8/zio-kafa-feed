# Kafka Publisher and Reader

This is a project that demonstrates publishing to Kafka and reading messages off of it via poll and seek.

The publishing is done via the IngestionJob.scala runnable and the reading is done via API calls structured as such:

curl "localhost:8080/topic/people/0?count=10"

where 0 is the offset we want to start off of and count is the maximum amount of messages we want to read off of each partition.

## Installation

Need scala and docker. Then run docker-compose up and the necessary kafka infrastructure is running on your machine!

## Tools

This was a foray into the ZIO world of things for Kafka.
Unfortunately ZIO Kafka consumer does not have the poll, seek that I desired for this project, but wrapping the code
around proved to be a fun challenge! Layers are not so scary it seems.

## TODO

Make the MessageReader an abstract class that can stylishly map into a response when given the expected type (and a decoder)
of the message. I.e. PersonReader = MessageReader[Person]
