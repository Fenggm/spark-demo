package com.fgm.streaming_kafka

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
  *利用sparkSteaming整合kafka
  *
  * @Auther: fgm
  */
object SparkStreamingKafkaReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("kafkaReceiver").setMaster("local[4]").set("spark.streaming.receiver.writeAheadLog.enable","true")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(5))
    //设置checkpoint目录，保证最原始的数据安全性
    ssc.checkpoint("./sparkReceiver")
    //指定zk服务地址
    val zkQuorum="node01:2181,node02:2181,node03:2181"
    //指定消费者组
    val groupId="spark-receiver"
    //指定topic相关信息
    val topics=Map("kafkaSpark"->1)
    //实现多个receiver接收器接收数据，加快数据接收的速度
    val dstreamList: immutable.IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to 3).map(x => {
      val createStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
      createStream
    })
    val unionKafkaDstream: DStream[(String, String)] = ssc.union(dstreamList)
    //获取kafka中topic中的数据
    val data: DStream[String] = unionKafkaDstream.map(_._2)
    //切分
    val words: DStream[String] = data.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()


  }

}
