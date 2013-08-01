package com.ansvia.nsqie

import net.liftweb.json._

object Test {

    def main(args:Array[String]){

        val nsq = NsqClient.create("MindtalkClient", "MindtalkClientApp", "localhost:4161", "mindtalk")

        nsq.subscribe("mindtalk", "nsqie"){ case (topic, channel, msg) =>
            println("got data %s from topic %s in channel %s".format(msg, topic, channel))
            MessageHandleReturn.SUCCESS
        }

    }

}
