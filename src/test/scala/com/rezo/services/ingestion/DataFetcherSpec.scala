package com.rezo.services.ingestion

import com.rezo.exceptions.Exceptions.{FailedToParseFile, FailedToReadFile}
import com.rezo.objects.Person
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import zio.*
import zio.test.*
import zio.test.Assertion.*

object DataFetcherSpec extends ZIOSpecDefault {

  private case class CustomData(foo: String, bar: Int)
  private object CustomData {
    implicit val decoder: Decoder[CustomData] = deriveDecoder[CustomData]
    implicit val encoder: Encoder[CustomData] = deriveEncoder[CustomData]
  }

  def spec: Spec[Any, Throwable] = suite("DataFetcherLiveSpec")(
    test(
      "fetchData should parse valid JSON and return a list of valid data"
    ) {
      val fileName = "random-data.json"
      for {
        data <- DataFetcherLive().fetchData[CustomData](fileName)
        correctlyParsed = data.collect { case Right(person) =>
          person
        }
        incorrectlyParsed = data.collect { case Left(failure) =>
          failure
        }
      } yield {
        assert(data)(hasSize(equalTo(5))) &&
        assert(correctlyParsed)(hasSize(equalTo(3))) &&
        assert(incorrectlyParsed)(hasSize(equalTo(2)))
      }
    },
    test("fetchPeople should fail with FailedToParseFile on invalid JSON") {
      val fileName = "invalid.json"
      for {
        result <- DataFetcherLive().fetchData[Person](fileName).exit
      } yield assert(result)(fails(isSubtype[FailedToParseFile](anything)))
    },
    test("fetchPeople should fail with FailedToReadFile if file is missing") {
      val fileName = "missing.json"
      for {
        result <- DataFetcherLive().fetchData[Person](fileName).exit
      } yield assert(result)(fails(isSubtype[FailedToReadFile](anything)))
    }
  )
}
