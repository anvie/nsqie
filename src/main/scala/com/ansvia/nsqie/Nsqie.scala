package com.ansvia.nsqie

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.{FailedFastException, ChannelClosedException, WriteException, Service}
import java.net.InetSocketAddress
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.conversions.time._
import com.twitter.util.Await
import java.nio.{ByteOrder, ByteBuffer}
import com.ansvia.commons.logging.Slf4jLogger
import com.twitter.finagle.http.RequestBuilder
import org.jboss.netty.util.CharsetUtil
import net.liftweb.json._
import com.twitter.util.Throw
import scala.collection.mutable.ArrayBuffer
import net.liftweb.json.JsonAST.{JInt, JString}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.jboss.netty.buffer.ChannelBuffers
import scala.collection.mutable

/**
 * Author: robin
 * Date: 8/1/13
 * Time: 2:51 PM
 *
 */

// factory
object NsqClient extends Slf4jLogger {
    def create(name:String, longName:String, lookupHost:String, topic:String) = {
        val httpClient: Service[HttpRequest, HttpResponse] = HttpClient.createClient(lookupHost)

        val resp:HttpResponse = Await.result(
            httpClient(RequestBuilder().url("http://" + lookupHost + "/lookup?topic=" + topic).buildGet()))

        debug("nsqd resp: " + resp)
        val content = resp.getContent.toString(CharsetUtil.UTF_8)
        debug("content: " + content)
        val json = parse(content)
        var producers = ArrayBuffer.empty[String]
        for {
            JField("broadcast_address", JString(producerHost)) <- json
            JField("tcp_port", JInt(producerPort)) <- json
        }{
            producers :+= producerHost + ":" + producerPort
        }
        debug("producers of %s: %s".format(topic, producers.toList))

        httpClient.close()
        NsqClient(producers.toList.head, name, longName, 1000)
    }

    private val httpClientsPoll = new
            mutable.HashMap[String, Service[HttpRequest, HttpResponse]]()
        with mutable.SynchronizedMap[String, Service[HttpRequest, HttpResponse]]

    def publish(host:String, topic:String, data:String){
        synchronized {
            val httpClient = httpClientsPoll.getOrElseUpdate(host,
                HttpClient.createClient(host))
            val buff = ChannelBuffers.buffer(data.getBytes.length)
            buff.writeBytes(data.getBytes)
            httpClient(RequestBuilder().url("http://%s/put?topic=%s".format(host, topic))
                .buildPost(buff))
        }
    }

    def cleanup(){
        synchronized {
            for ( c <- httpClientsPoll.values ){
                c.close()
            }
            httpClientsPoll.clear()
        }
    }
}

case class NsqClient(hostNPort:String, shortId:String, longId:String, rdyCount:Int) extends Slf4jLogger {

    @volatile
    var _rdyCount = rdyCount

    private val s = hostNPort.split(":")
    private val host = s(0)
    private val port = s(1).toInt
    private var inited = false
    var connected = false
    private var retrier:Runnable = _

    private var client:Service[String, Object] = buildClient()

    /**
     * (topic, channel, msg)
     */
    type MessageHandler = (String, String, NSQMessage) => Int
    case class SubscribeContext(topic:String, channel:String, mh:MessageHandler)

    import NsqCommands._

    def publish(host:String, topic:String, data:String) =
        NsqClient.publish(host, topic, data)

    def subscribe[T](topic:String, channel:String)(implicit mh:MessageHandler){
        ensureInit()
        _dispatch(SUB + " " + topic + " " + channel + "\n").onSuccess {
            case OK =>
                implicit val ctx = SubscribeContext(topic, channel, mh)
                if (retrier == null){
                    retrier = new Runnable {
                        def run() {
                            debug("retrying...")
                            subscribe(topic, channel)(mh)
                        }
                    }
                }
                rdy(_rdyCount).onSuccess(feed)
            case x =>
                error("cannot subscribe. returned from nsqd: " + x)
        }

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
                connected = true
                debug("connected: " + connected)
            case x =>
                error("cannot identify, returned from nsqd: " + x)
        }
    }

    private def _dispatch(cmd:String, data:Option[String]=None)={
        ensureInit()
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

    private def reset(){
        connected = false
        inited = false
        client.close()
        client = buildClient()

        client("  V2").onSuccess { data => data match {
                case OK =>
                    connected = true
                    debug("connected: " + connected)
                    if (retrier != null){
                        retrier.run()
                    }
                case x =>
                    error("cannot identify, returned from nsqd: " + x)
            }
        }
    }

    private def identifyInternal(){
        identify("""{"short_id":"%s","long_id":"%s"}""".format(shortId, longId))
    }

    private def ensureInit(){
        if (!inited){

            identifyInternal()

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
                nop()
            case x =>
                debug("feed got unknown data: " + x)
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
        _dispatch(REQ + " " + msg.id + " 100\n").onSuccess(feed)
    }

    def rdy(count:Int)(implicit ctx:SubscribeContext) = {
        _dispatch("RDY %d\n".format(count))
    }

    def nop()(implicit ctx:SubscribeContext){
        _dispatch(NOP + "\n")
            .onSuccess(feed)
            .onFailure {
                case e =>
                    error(e.getMessage)
                    inited = false
                    ensureInit()
            }
    }

    private def buildClient() = {
        ClientBuilder()
            .codec(NSQCodec)
            .hosts(new InetSocketAddress(host, port))
            .retryPolicy(RetryPolicy.backoff(Backoff.exponential(1 seconds, 2) take 15) {
                case Throw(x: WriteException) => true
                case Throw(x) =>
                    error("connection failed, e: " + x.getMessage)
                    reset()
                    false
            })
            .hostConnectionLimit(1)
//            .timeout(15 seconds)
            .build()
    }


}


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
