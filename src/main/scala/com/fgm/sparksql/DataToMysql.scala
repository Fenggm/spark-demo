package com.fgm.sparksql

import java.util.Properties
import org.apache.spark.sql.SparkSession

/**
  *通过sparksql把结果数据写入到mysql表
  * @Auther: fgm
  */
case class User(val id:Int,val name:String,val age:Int)

object DataToMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataToMysql").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //读取数据文件
    val RDD1 = sc.textFile("D:\\tmp\\user.txt").map(_.split(" "))
    //将RDD与样例类关联
    val userRDD = RDD1.map(x=>User(x(0).toInt,x(1),x(2).toInt))
    //构建DataFrame
    import spark.implicits._
    val df = userRDD.toDF()
    df.printSchema()
    df.show()

    df.createTempView("user")
    val result = spark.sql("select * from user where age >30")
    //定义表名
    val table="user"
    //将结果写入到mysql
    //定义数据库url
    val url="jdbc:mysql://localhost:3306/spark"
    //properties
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123")

    result.write.mode("append").jdbc(url,table,properties)

    //再将数据库中的数据读取出来，检查是否写入成功,也可以进行其他相关操作
    val jdbc=spark.read.jdbc(url,table,properties)
    jdbc.show()

    spark.stop()
  }
}
