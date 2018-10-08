package com.fgm.sparksql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  *通过StructType指定schema,将rdd转换成dataFrame
  * @Auther: fgm
  */
object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    //创建SparkSession对象
    val spark = SparkSession.builder().appName("StructTypSchema").master("local[2]").getOrCreate()
    //创建SparkContext
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //读取数据
    val rdd1 = sc.textFile("D:\\tmp\\person.txt").map(_.split(" "))
    //将rdd与rowd对象关联
    val rowRDD = rdd1.map(x=>Row(x(0).toInt,x(1),x(2).toInt))

    //指定schema
    val schema=(new StructType).add(StructField("id",IntegerType,true))
      .add(StructField("name",StringType,false))
      .add(StructField("age",IntegerType,true))

    val dataFrame = spark.createDataFrame(rowRDD,schema)
    dataFrame.printSchema()
    dataFrame.show()

    dataFrame.createTempView("person")
    spark.sql("select * from person").show()

    spark.stop()


  }

}
