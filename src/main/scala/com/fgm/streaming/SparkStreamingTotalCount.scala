package com.fgm.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *利用sparkStreaming接收socket数据，实现所有批次单词出现的结果累加
  *
  * @Auther: fgm
  */
object SparkStreamingTotalCount {

  def updateFunction(newValues:Seq[Int], counts:Option[Int]): Option[Int] = {
    val newCount=counts.getOrElse(0)+newValues.sum
    Some(newCount)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreamingTotalCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    
    val ssc = new StreamingContext(sc,Seconds(10))
    //设置checkpoint目录，保存之前批次的单词统计结果
    ssc.checkpoint("./checkpoint")

    val socketTextStream = ssc.socketTextStream("node01",9999)
    val words = socketTextStream.flatMap(_.split(" "))
    val wordAndOne = words.map((_,1))
    val result = wordAndOne.updateStateByKey(updateFunction)

    result.print()

    ssc.start()
    ssc.awaitTermination()
    
    
    
    
    
    
    
    
  }
}
