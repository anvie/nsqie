package com.ansvia.nsqie

import com.twitter.finagle.CodecFactory
import com.twitter.finagle.Codec
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.string.StringEncoder
import org.jboss.netty.util.CharsetUtil

/**
 * Author: robin
 * Date: 8/1/13
 * Time: 12:32 PM
 *
 */

object NSQCodec extends NSQCodec

class NSQCodec extends CodecFactory[String, NSQFrame] {
    def client = Function.const {
        new Codec[String, NSQFrame]{
            def pipelineFactory = new ChannelPipelineFactory {
                def getPipeline = {
                    val pipeline = Channels.pipeline()
                    pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
                    pipeline.addLast("frameDecoder", new NSQFrameDecoder)
                    pipeline
                }
            }
        }
    }

    def server = Function.const {
        new Codec[String, NSQFrame]{
            def pipelineFactory = new ChannelPipelineFactory {
                def getPipeline = {
                    val pipeline = Channels.pipeline()
//                    pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
                    pipeline.addLast("frameDecoder", new NSQFrameDecoder)
                    pipeline
                }
            }
        }
    }
}
