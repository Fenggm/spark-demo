package com.fgm.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object PV {
  def main(args: Array[String]): Unit = {
    //创建sparkconf，设置appName
    val sparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //加载数据文件
    val data = sc.textFile("D:\\access.log")
    //统计pv
    val pv = data.count()
    println(pv)

    //关闭sc
    sc.stop()


  }

}
