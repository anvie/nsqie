Scala [NSQ](http://bitly.github.io/nsq/) client built on top of [Finagle](https://github.com/twitter/finagle)

Example Usage
-----------------

```scala
val nsq = NsqSubscriber("Mindtalk", "MindtalkApp",
    "127.0.0.1:4161", "mindtalk", "nsqie")

nsq.listen { case (topic, channel, msg) =>
    println("got data %s from topic %s in channel %s".format(msg, topic, channel))
    MessageHandleReturn.SUCCESS
}

NsqClient.publish("127.0.0.1:4151", "mindtalk",
    "hello " + System.currentTimeMillis() + " :P")
```

Under heavy development, don't use this in production.

[] Robin Sy.
