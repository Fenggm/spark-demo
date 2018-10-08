package com.fgm.streaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @Auther: fgm
  */
object SparkStreamingWindow {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("sparkStreamingWindow").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc,Seconds(5))
    val lines = ssc.socketTextStream("node01",9999)
    val words = lines.flatMap(_.split(" "))
    val wordAndOne = words.map((_,1))
    val result:DStream[(String,Int)] = wordAndOne.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(10))

    result.print()

    ssc.start()
    ssc.awaitTermination()









  }
}
