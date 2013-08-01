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
import org.slf4j.LoggerFactory
import com.ansvia.commons.logging.Slf4jLogger

object Nsqie {

    implicit val formats = DefaultFormats

    val log = LoggerFactory.getLogger(getClass)

    object NsqCommands {
        val SUB = "SUB"
        val NOP = "NOP"
        val RDY = "RDY"
        val HEARTBEAT = "_heartbeat_"
        val MAGIC = "  V2"
        val OK = "OK"
        val FIN = "FIN"
        val REQ = "REQ"
    }
    object MessageHandleReturn {
        val SUCCESS = 0
        val UNKNOWN_ERROR = 1
    }
    class SkipException extends Throwable
    case class NsqClient(hostNPort:String, shortId:String, longId:String, rdyCount:Int) extends Slf4jLogger {

        @volatile
        var _rdyCount = rdyCount

        private val s = hostNPort.split(":")
        private val host = s(0)
        private val port = s(1).toInt
        lazy val client:Service[String, Object] = ClientBuilder()
            .codec(NSQCodec)
            .hosts(new InetSocketAddress(host, port))
            .retryPolicy(RetryPolicy.backoff(Backoff.exponential(1 seconds, 2) take 15) {
                case Throw(x: WriteException) => true
                case Throw(x) =>
                    error("connection failed, e: " + x)
                    true
            })
            .hostConnectionLimit(1)
            .build()

        /**
         * (topic, channel, msg)
         */
        type MessageHandler = (String, String, NSQMessage) => Int
        case class SubscribeContext(topic:String, channel:String, mh:MessageHandler)

        import NsqCommands._

        def subscribe[T](topic:String, channel:String)(implicit mh:MessageHandler){
            ensureInit()

            _dispatch(SUB + " " + topic + " " + channel + "\n").onSuccess {
                case OK =>
                    implicit val ctx = SubscribeContext(topic, channel, mh)
                    _dispatch("RDY %d\n".format(_rdyCount)).onSuccess(feed)
                case x =>
                    error("cannot subscribe. returned from nsqd: " + x)
            }

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
            val bf = ByteBuffer.allocate(13 + 4 + data.getBytes.length).order(ByteOrder.BIG_ENDIAN)

            bf.put("  V2IDENTIFY\n".getBytes)
            bf.putInt(data.length)
            bf.put(data.getBytes)

            val payload = new String(bf.array())
            debug("payload: " + payload)
            Await.result(client(payload)) match {
                case OK =>
                case x =>
                    error("cannot identify, returned from nsqd: " + x)
            }
        }

        private def _dispatch(cmd:String, data:Option[String]=None)={
            var payload = cmd
            data.map { d =>
                val bf = ByteBuffer.allocate(d.length + 6).order(ByteOrder.BIG_ENDIAN)
                bf.putInt(d.length)
                bf.put(d.getBytes)
                payload = payload + new String(bf.array())
            }
            debug("payload: " + payload)
            client(payload)
        }

        private var inited = false
        private def ensureInit(){
            if (!inited){

                identify("""{"short_id":"%s","long_id":"%s"}""".format(shortId, longId))

                inited = true
            }
        }

        def feed(data:Object)(implicit ctx:SubscribeContext){
            data match {
                case NSQFrame(frameType, size, msg) =>
                    synchronized {
                        _rdyCount -= 1
                    }
                    debug("feed got message: " + msg)
                    debug("rdy count: " + _rdyCount)
                    handleMessage(msg)
                case HEARTBEAT =>

                case x =>
                    debug("feed got: " + x)
            }
        }

        /**
         * Override this as you wish.
         * @param msg message handler.
         */
        def handleMessage(msg:NSQMessage)(implicit ctx:SubscribeContext){
            ctx.mh(ctx.topic, ctx.channel, msg) match {
                case MessageHandleReturn.SUCCESS =>
                    markSucceed(msg)
                case _ =>
                    requeue(msg)
            }
        }

        def markSucceed(msg:NSQMessage)(implicit ctx:SubscribeContext){
            _dispatch(FIN + " " + msg.id + "\n").onSuccess(feed)
        }

        def requeue(msg:NSQMessage)(implicit ctx:SubscribeContext){
            _dispatch(REQ + " " + msg.id + " 100\n")
        }

        def nop()(implicit ctx:SubscribeContext){
            _dispatch(NOP + "\n").onSuccess(feed)
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

            val nsq = NsqClient(producers.toList.head, "MindtalkClient", "MindtalkClientApp", 100)
            nsq.subscribe("mindtalk", "nsqie"){ case (_topic, channel, msg) =>
                println("got data %s from topic %s in channel %s".format(msg, _topic, channel))
                MessageHandleReturn.SUCCESS
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
