Scala [NSQ](http://bitly.github.io/nsq/) client built on top of [Finagle](https://github.com/twitter/finagle)

Example Usage
-----------------

```scala
val nsq = NsqClient.create("MindtalkClient", "MindtalkClientApp", "localhost:4161", "mindtalk")

nsq.subscribe("mindtalk", "nsqie"){ case (topic, channel, msg) =>
    println("got data %s from topic %s in channel %s".format(msg, topic, channel))
    MessageHandleReturn.SUCCESS
}
```

Under heavy development, don't use this in production.

[] Robin Sy.
