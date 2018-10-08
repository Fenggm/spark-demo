package com.fgm.streaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *通过sparkStreaming实现单词统计
  * @Auther: fgm
  */
object SparkStreamingWordCount {
  def main(args: Array[String]): Unit = {
    //配置sparkConf
    val sparkConf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[2]")
    //构建sparkContext对象
    val sc = new SparkContext(sparkConf)
    //设置日志级别
    sc.setLogLevel("WARN")
    //构建StreamingContext对象，每个批处理的时间间隔
    val scc:StreamingContext = new StreamingContext(sc,Seconds(5))
    //注册一个监听的IP地址和端口，收集数据
    val lines = scc.socketTextStream("node01",9999)
    //切分每一行记录
    val words = lines.flatMap(_.split(" "))
    val wordAndOne:DStream[(String,Int)] = words.map((_,1))
    val result = wordAndOne.reduceByKey(_+_)

    //打印数据,outputOperations，会触发运行
    result.print()
    //开启流式计算
    scc.start()
    //等待计算结束
    scc.awaitTermination()



  }


}
