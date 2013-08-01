package com.ansvia.nsqie


object Test {

    def main(args:Array[String]){

        val nsq = NsqSubscriber("Mindtalk", "MindtalkApp", "127.0.0.1:4161", "mindtalk", "nsqie")

        nsq.listen { case (topic, channel, msg) =>
            println("got data %s from topic %s in channel %s".format(msg, topic, channel))
            MessageHandleReturn.SUCCESS
        }

        NsqClient.publish("127.0.0.1:4151", "mindtalk",
            "hello " + System.currentTimeMillis() + " :P")

    }

}
