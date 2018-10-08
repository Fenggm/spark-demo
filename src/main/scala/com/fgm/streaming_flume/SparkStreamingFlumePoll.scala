package com.fgm.streaming_flume

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *sparkStreaming整合flume，采用拉模式
  * @Auther: fgm
  */
object SparkStreamingFlumePoll {

  def updateFunc(currentValues:Seq[Int], historyValues:Option[Int]):Option[Int] = {

    val newValue: Int = currentValues.sum+historyValues.getOrElse(0)
    Some(newValue)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FlumePoll").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./flume-poll")
    //通过flumeUtils调用createPollingStream方法获取flume中的数据
    val pollingStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,"node01",8888)
    //获取flume中event的body
    val data: DStream[String] = pollingStream.map(x=>new String(x.event.getBody.array()))
    val words: DStream[String] = data.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)

    //输出
    result.print()

    //开启流式计算
    ssc.start()
    ssc.awaitTermination()

  }
}
