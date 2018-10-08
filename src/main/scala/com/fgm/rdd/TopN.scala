package com.fgm.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo:利用spark实现点击流日志分析===TOPN
object TopN {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val data = sc.textFile("D:\\access.log")
    val urlAndOne:RDD[(String,Int)] = data.filter(_.split(" ").length>10).map(x=>(x.split(" ")(10),1))

    //相同url出现的累加1
    val result:RDD[(String,Int)] = urlAndOne.reduceByKey(_+_)

    //排序
    val sortedRDD = result.sortBy(_._2,false)

    //取出出现次数最多的前5位
    val top5 = sortedRDD.take(5)


    top5.foreach(println)

    sc.stop()






  }

}
