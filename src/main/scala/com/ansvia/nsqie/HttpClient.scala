package com.ansvia.nsqie

import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http

/**
 * Author: robin
 * Date: 7/31/13
 * Time: 11:56 PM
 *
 */
object HttpClient {

    def createClient(hosts:String, connLimit:Int=1):Service[HttpRequest, HttpResponse] =
        ClientBuilder()
            .codec(Http())
            .hosts(hosts)
            .hostConnectionLimit(connLimit)
            .build()

}
