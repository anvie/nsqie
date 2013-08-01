package com.ansvia.nsqie

import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.{ChannelBufferOutputStream, AbstractChannelBuffer, ChannelBuffers, ChannelBuffer}
import com.ansvia.commons.logging.Slf4jLogger
import java.nio.ByteBuffer

/**
 * Author: robin
 * Date: 8/1/13
 * Time: 3:16 AM
 *
 */
class NSQFrameDecoder extends FrameDecoder with Slf4jLogger {
    import FrameType._
    import Commands._
    import ResponseType._

    def handleResponse(channel: Channel, buffer: ChannelBuffer, size: Int):String = {
        val resp = readString(buffer, size)
        if (resp != null){
            debug("handle response: " + resp)
            resp match {
                case HEARTBEAT =>
                    sendNOP(channel)
                    HEARTBEAT
                case _ =>
            }
        }
        resp
    }

    def handleError(channel: Channel, buffer: ChannelBuffer, size: Int) = {
        readString(buffer, size)
    }

    def handleMessage(channel: Channel, buffer: ChannelBuffer, size: Int):NSQFrame = {
        val ts = buffer.readLong()
        val attempts = buffer.readUnsignedShort()
        val msgId = new Array[Byte](16)
        buffer.readBytes(msgId)
        val body = new Array[Byte](size - 4 - NSQMessage.MIN_SIZE_BYTES)
        buffer.readBytes(body)
        val msg = NSQMessage(ts, attempts, msgId, body)
        val frame = NSQFrame(MESSAGE, size, msg)
        debug("decoded msg with id: " + new String(msgId))
        frame
    }

    private def readString(buffer:ChannelBuffer, size:Int) = {
        val bb = ByteBuffer.allocate(size - 4)
        buffer.readBytes(bb)
        new String(bb.array())
    }

    def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer):Object = {
        val readableBytes = buffer.readableBytes()
        if (readableBytes > 4){

            buffer.markReaderIndex()
            val size = buffer.readInt()

            if (readableBytes >= size + 4){

                val cbuff = ChannelBuffers.buffer(size)

                buffer.readBytes(cbuff)

                val frameType = cbuff.readInt()

//                assert(frameType != 0, "frame type is null")

                frameType match {
                    case RESPONSE =>
                        handleResponse(channel, cbuff, size)

                    case ERROR =>
                        handleError(channel, cbuff, size)

                    case MESSAGE =>
                        handleMessage(channel, cbuff, size)
                }

            }else{
                buffer.resetReaderIndex()
                null
            }

        }else{
            null
        }
    }

    private def sendNOP(implicit channel:Channel){
        write(Nop)
    }

    private def write(cmd:NSQCommand)(implicit channel:Channel){
        val buff = ChannelBuffers.buffer(cmd.size)
        channel.write(buff)
    }
}

object FrameType {
    val RESPONSE = 0
    val ERROR = 1
    val MESSAGE = 2
}

object ResponseType {
    val OK = "OK"
    val INVALID = "E_INVALID"
    val BAD_TOPIC = "E_BAD_TOPIC"
    val BAD_BODY = "E_BAD_BODY"
    val BAD_CHANNEL = "E_BAD_CHANNEL"
    val BAD_MESSAGE = "E_BAD_MESSAGE"
    val PUT_FAILED = "E_PUT_FAILED"
    val FINISH_FAILED = "E_FIN_FAILED"
    val REQUIRE_FAILED = "E_REQ_FAILED"
    val CLOSE_WAIT = "CLOSE_WAIT"
    val HEARTBEAT = "_heartbeat_"
}

case class NSQFrame(frameType:Int, size:Int, msg:NSQMessage) {

}

object NSQMessage  {
    val MIN_SIZE_BYTES = 26
}
case class NSQMessage(ts:Long, attempts:Int, messageBody:Array[Byte], body:Array[Byte]){
    import NSQMessage._
    def getSize = MIN_SIZE_BYTES + body.length
    override def toString = "<%s, %s>".format(id, bodyStr)
    def messageBodyStr = new String(messageBody)
    def bodyStr = new String(body)
    def id = messageBodyStr
}

object Commands {
    trait NSQCommand {
        def toBytes:Array[Byte]
        def size:Int
    }

    object Nop extends NSQCommand {
        override def toString = "NOP\n"
        def toBytes = toString.getBytes
        def size = toBytes.length
    }
}