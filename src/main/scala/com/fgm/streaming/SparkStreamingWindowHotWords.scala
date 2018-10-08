package com.fgm.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *开窗函数统计一定时间内的热门词汇
  *
  * @Auther: fgm
  */
object SparkStreamingWindowHotWords {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HotWords").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //创建StreamingContext对象
    val ssc = new StreamingContext(sc,Seconds(5))
    val lines = ssc.socketTextStream("node01",9999)
    val words = lines.flatMap(_.split(" "))
    val wordAndOne = words.map((_,1))

    val result = wordAndOne.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(5))
    val data = result.transform(rdd => {
      val dataRDD = rdd.sortBy(_._2, false)
      val sortResult = dataRDD.take(5)
      sortResult.foreach(println)
      dataRDD

    })
    data.print()
    ssc.start()
    ssc.awaitTermination()



  }

}
