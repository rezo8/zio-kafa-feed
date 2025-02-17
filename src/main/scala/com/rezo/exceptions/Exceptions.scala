package com.rezo.exceptions

import zio.http.Status

object Exceptions {
  class ConfigLoadException extends Exception
  trait RezoException extends Exception

  trait ServerException extends RezoException {
    def status: Status.Error = Status.InternalServerError
  }

  trait IngestionException extends RezoException

  final case class ParseException(error: String) extends IngestionException {}
}
