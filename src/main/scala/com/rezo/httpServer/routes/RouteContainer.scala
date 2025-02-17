package com.rezo.httpServer.routes

import zio.*
import zio.http.*


trait RouteContainer {
  def routes: Routes[Any, Response]
}
