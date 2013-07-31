package com.ansvia.nsqie

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.{WriteException, Service}
import java.net.InetSocketAddress
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.conversions.time._
import com.twitter.util.{Await, Throw}
import com.twitter.finagle.http.RequestBuilder
import org.jboss.netty.util.CharsetUtil
import net.liftweb.json._
import scala.collection.mutable.ArrayBuffer
import java.nio.{ByteOrder, ByteBuffer}
import scala.annotation.tailrec

object Nsqie {

    implicit val formats = DefaultFormats

    object NsqCommands {
        val SUB = "SUB"
        val NOP = "NOP"
        val RDY = "RDY"
        val HEARTBEAT = "_heartbeat_"
        val MAGIC = "  V2"
        val OK = "OK"
    }
    class SkipException extends Throwable
    case class NsqClient(hostNPort:String){
        private val s = hostNPort.split(":")
        private val host = s(0)
        private val port = s(1).toInt
        lazy val client:Service[String, String] = ClientBuilder()
            .codec(StringCodec)
            .hosts(new InetSocketAddress(host, port))
            .retryPolicy(RetryPolicy.backoff(Backoff.exponential(1 seconds, 2) take 15) {
                case Throw(x: WriteException) => true
                case Throw(x) =>
                    println("connection failed, e: " + x)
                    true
            })
            .hostConnectionLimit(1)
            .build()

        import NsqCommands._
        def subscribe[T](topic:String, channel:String)(func: (String, String, String) => Unit){
            ensureInit()
//            println(SUB + " " + topic + " " + channel + "\n")
//
//            client(SUB + " " + topic + " " + channel + "\n").onSuccess { resp =>
//                try {
//                    handler(resp)
//                    func(topic, channel, resp)
//                }catch{
//                    case e:SkipException =>
//                }
//            }
//
//
//            println("set RDY to 100")
//            client(RDY + " 100\n")

        }

        def handler:PartialFunction[String, Unit] = {
            case HEARTBEAT =>
                println("sending NOP")
                client(NOP + "\n")
                throw new SkipException
            case "OK" =>
                throw new SkipException
            case _ =>
        }

        def identify(data:String) = {
            val bf = ByteBuffer.allocate(data.length + 29).order(ByteOrder.BIG_ENDIAN)

            bf.put("IDENTIFY\n".getBytes)
            bf.putInt(data.length)
            bf.put(data.getBytes)

            val payload = new String(bf.array())
            println("payload: " + payload)
            Await.result(client(payload))
        }

        private def _dispatch(cmd:String, data:Option[String]=None)={
            var payload = cmd
            data.map { d =>
                val bf = ByteBuffer.allocate(d.length + 6).order(ByteOrder.BIG_ENDIAN)
                bf.putInt(d.length)
                bf.put(d.getBytes)
                payload = payload + new String(bf.array())
            }
            println("payload: " + payload)
            client(payload)
        }

        private var inited = false
        private def ensureInit(){
            if (!inited){
//                client("  V2").onSuccess { data => data.trim match {
//                        case HEARTBEAT =>
//                            _dispatch(NOP)
//                        case x =>
//                            println("x: " + x)
//                    }
//                }
//
//                println(identify("""{"short_id":"nsqie","long_id":"nsqie"}"""))

                _dispatch(MAGIC + "SUB mindtalk nsqie\n").onSuccess { data =>
                    data.trim match {
                        case OK =>
                            _dispatch(RDY + " 1")
                    }
                }

                inited = true
            }
        }
    }

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

            val nsq = NsqClient(producers.toList.head)
            nsq.subscribe("mindtalk", "nsqie"){ case (_topic, channel, data) =>
                println("got data %s from topic %s in channel %s".format(data, _topic, channel))
            }
        }

//
//        val client:Service[String, String] = ClientBuilder()
//            .codec(StringCodec)
//            .hosts(new InetSocketAddress(8123))
//            .retryPolicy(RetryPolicy.backoff(Backoff.exponential(1 seconds, 2) take 15) {
//                case Throw(x: WriteException) => true
//                case Throw(x) =>
//                    println("connection failed, e: " + x)
//                    true
//            })
//            .hostConnectionLimit(1)
//            .build()
//
//        client("hi robin\n") onSuccess { result =>
//            println("received: " + result)
//        } onFailure { error =>
//            error.printStackTrace()
//        } ensure {
//            client.close()
//        }


    }

}
