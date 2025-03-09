package com.rezo.exceptions

import java.io.IOException

object Exceptions {
  class ConfigLoadException extends Exception

  final case class FailedToReadFile(ioException: IOException) extends Exception

  final case class FailedToParseFile(error: io.circe.Error) extends Exception
}
