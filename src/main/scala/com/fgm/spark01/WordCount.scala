package com.fgm.spark01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo:利用scala语言开发spark的wordcount程序
object WordCount {
  def main(args: Array[String]): Unit = {
    //1 创建sparkConf对象，设置appName和master的地址
    val sparkConf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    //创建sparkContext对象，它是所有spark 程序的入口
    val sc = new SparkContext(sparkConf)
    //设置日志输出级别
    sc.setLogLevel("WARN")

    //读取数据文件
    val data:RDD[String] = sc.textFile("D:\\a1.txt")
    //4 切分每一行，获取所有的单词
    val words:RDD[String] = data.flatMap(_.split(" "))
    //每个单词记为1
    val wordAndOne:RDD[(String,Int)] = words.map(x=>(x,1))
    //相同单词出现的1累加
    val result = wordAndOne.reduceByKey(_+_)
    //收集数据
    val finalResult = result.collect()
    //打印输出
    finalResult.foreach(println)
    //关闭sc
    sc.stop()


  }


}

object WordCount2{
  def main(args: Array[String]): Unit = {
    //1 创建sparkConf对象，设置appName和master的地址
    val sparkConf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    //创建sparkContext对象，它是所有spark 程序的入口
    val sc = new SparkContext(sparkConf)
    //设置日志输出级别
    sc.setLogLevel("WARN")
    val array: Array[(String, Int)] = sc.textFile("D:\\a1.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect()
    array.foreach(println)
    sc.stop()

  }
}
