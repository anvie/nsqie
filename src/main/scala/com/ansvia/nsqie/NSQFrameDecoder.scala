package com.ansvia.nsqie

import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

/**
 * Author: robin
 * Date: 8/1/13
 * Time: 3:16 AM
 *
 */
class NSQFrameDecoder extends FrameDecoder {
    import FrameType._

    def handleResponse(channel: Channel, buffer: ChannelBuffer, i: Int):NSQFrame = {

    }

    def handleError(channel: Channel, buffer: ChannelBuffer, i: Int):NSQFrame = {

    }

    def handleMessage(channel: Channel, buffer: ChannelBuffer, i: Int):NSQFrame = {

    }

    def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer) = {
        val readableBytes = buffer.readableBytes()
        if (readableBytes > 4){

            buffer.markReaderIndex()
            val size = buffer.readInt()

            if (readableBytes > size + 4){

                val cbuff = ChannelBuffers.buffer(size)

                buffer.readBytes(cbuff)

                val frameType = cbuff.readInt()

                assert(frameType != null, "frame type is null")

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
}

object FrameType {
    val RESPONSE = 0
    val ERROR = 1
    val MESSAGE = 2
}

case class NSQFrame(frameType:Int, size:Int, msg:NSQMessage) {

}

object NSQMessage  {
    val MIN_SIZE_BYTES = 26
}
case class NSQMessage(ts:Long, attempts:Int, messageBody:Array[Byte], body:Array[Byte]){
    import NSQMessage._
    def getSize = MIN_SIZE_BYTES + body.length
}