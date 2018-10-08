package com.fgm.rdd

import org.apache.spark.{SparkConf, SparkContext}

object UV {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //加载文件
    val data = sc.textFile("D:\\access.log")
    //获取所有IP的值，并且去重
    val ips = data.map(_.split(" ")(0))
    val distinctRDD = ips.distinct()
    val uv = distinctRDD.count()
    println(uv)

    sc.stop()










  }

}
