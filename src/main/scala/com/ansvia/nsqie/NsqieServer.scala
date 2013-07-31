package com.ansvia.nsqie

import com.twitter.finagle.Service
import com.twitter.util.Future
import com.twitter.finagle.builder.ServerBuilder
import java.net.InetSocketAddress

/**
 * Author: robin
 * Date: 7/31/13
 * Time: 11:20 PM
 *
 */
object NsqieServer {

    def main(args: Array[String]) {

        val service = new Service[String, String]{
            def apply(request: String) = Future.value(request)
        }

        val server = ServerBuilder()
            .codec(StringCodec)
            .bindTo(new InetSocketAddress(8123))
            .name("echoer")
            .build(service)

    }
}
