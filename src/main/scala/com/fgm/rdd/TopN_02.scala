package com.fgm.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TopN_02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val data = sc.textFile("D:\\access.log")
   // val url:RDD[(String,Int)] = data.filter(_.split(" ").length>10).map(x=>(x.split(" ")(10),1))

    val urlAndOne = data.filter(_.split(" ").length>10 ).map(x=>(x.split(" ")(10),1))

    //对为"-"的进行过滤
    val url = urlAndOne.filter(_._1!="\"-\"")
    //相同url出现的累加1

    val result:RDD[(String,Int)] = url.reduceByKey(_+_)

    //排序

    val sortedRDD = result.sortBy(_._2,false)

    //取出出现次数最多的前5位
    val top5 = sortedRDD.take(5)


    top5.foreach(println)

    sc.stop()













  }

}
