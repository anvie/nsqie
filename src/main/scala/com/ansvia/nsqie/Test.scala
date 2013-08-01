package com.ansvia.nsqie

import com.twitter.finagle.http.RequestBuilder
import org.jboss.netty.util.CharsetUtil
import net.liftweb.json._
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory

object Test {

    implicit val formats = DefaultFormats

    def main(args:Array[String]){

        val topic = "mindtalk"
        val httpClient = HttpClient.createClient("localhost:4161")
        httpClient(RequestBuilder().url("http://localhost:4161/lookup?topic=" + topic).buildGet())
            .onSuccess { resp =>
            println("nsqd resp: " + resp)
            val content = resp.getContent.toString(CharsetUtil.UTF_8)
            println("content: " + content)
            val json = parse(content)
            var producers = ArrayBuffer.empty[String]
            for {
                JField("broadcast_address", JString(producerHost)) <- json
                JField("tcp_port", JInt(producerPort)) <- json
            }{
                producers :+= producerHost + ":" + producerPort
            }
            println("producers of %s: %s".format(topic, producers.toList))

            val nsq = NsqClient(producers.toList.head, "MindtalkClient", "MindtalkClientApp", 100)
            nsq.subscribe("mindtalk", "nsqie"){ case (_topic, channel, msg) =>
                println("got data %s from topic %s in channel %s".format(msg, _topic, channel))
                MessageHandleReturn.SUCCESS
            }
        }

    }

}
