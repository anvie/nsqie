package com.ansvia.nsqie

import com.twitter.finagle.{Service, SimpleFilter, ServiceFactory, Codec, CodecFactory}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}

/**
 * Author: robin
 * Date: 7/31/13
 * Time: 11:22 PM
 *
 */

object StringCodec extends StringCodec

class StringCodec extends CodecFactory[String, String] {
    def client = Function.const {
        new Codec[String, String]{
            def pipelineFactory = new ChannelPipelineFactory {
                def getPipeline = {
                    val pipeline = Channels.pipeline()
                    pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
                    pipeline.addLast("stringDecode", new StringDecoder(CharsetUtil.UTF_8))
                    pipeline
                }
            }
//
//            override def prepareConnFactory(factory: ServiceFactory[String, String]) =
//                (new AddNewlineFilter) andThen factory
        }
    }

    def server = Function.const {
        new Codec[String, String]{
            def pipelineFactory = new ChannelPipelineFactory {
                def getPipeline = {
                    val pipeline = Channels.pipeline()
                    pipeline.addLast("frameDecoder", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
                    pipeline.addLast("stringDecode", new StringDecoder(CharsetUtil.UTF_8))
                    pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
                    pipeline
                }
            }
        }
    }

    class AddNewlineFilter extends SimpleFilter[String, String](){
        def apply(request: String, service: Service[String, String]) = service(request + "\n")
    }
}
