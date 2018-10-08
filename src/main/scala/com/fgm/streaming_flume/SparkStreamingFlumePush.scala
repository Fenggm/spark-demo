package com.fgm.streaming_flume

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *利用sparkStreaming整合flume
  * @Auther: fgm
  */
object SparkStreamingFlumePush {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("FlumePush").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val ssc: StreamingContext = new StreamingContext(sc,Seconds(0))
    val Stream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc,"192.168.52.5",8888)
    //获取flume中event
    val data: DStream[String] = Stream.map(x=>new String(x.event.getBody.array()))
    val words: DStream[String] = data.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()










  }

}
