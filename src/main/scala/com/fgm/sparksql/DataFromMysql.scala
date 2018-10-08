package com.fgm.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  *通过sparksql读取mysql表中的数据
  *
  * @Auther: fgm
  */
object DataFromMysql {
  def main(args: Array[String]): Unit = {
    //创建对象
    val spark = SparkSession.builder().appName("DataFromMysql").master("local[2]").getOrCreate()
    //通过sparkSession对象加载mysql中的数据
    val url="jdbc:mysql://localhost:3306/spark"
    //定义表名
    val table="test"
    //properties
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123")
    val jdbc = spark.read.jdbc(url,table,properties)

    jdbc.printSchema()
    jdbc.show()
    jdbc.createTempView("test")
    spark.sql("select * from test").show()
    spark.stop()
  }

}
