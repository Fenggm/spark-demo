package com.fgm.sparksql

import org.apache.spark.sql.SparkSession

//利用反射，将rdd转换成dataFrame
case class Person(val id:Int,val name:String,val age:Int)


object SchemaDemo {
  def main(args: Array[String]): Unit = {

    //创建SparkSession对象
    val sparkSession = SparkSession.builder().appName("Schema").master("local[2]").getOrCreate()
    //创建SparkContext对象
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    //读取数据文件
    val rdd1 = sc.textFile("D:\\tmp\\person.txt").map(_.split(" "))
    //将rdd与样例类关联
    val personRDD = rdd1.map(x=>Person(x(0).toInt,x(1),x(2).toInt))
    //将personRDD转换成DataFrame,需导入隐式转换
    import sparkSession.implicits._
    val personDF = personRDD.toDF()
    //dataFrame操作
    //DSL风格
    personDF.printSchema()
    personDF.show()
    personDF.select("name","age").show()
    personDF.select($"age">30).show()

    //sql风格语法
    personDF.createTempView("person")
    sparkSession.sql("select * from person").show()
    sparkSession.sql("select * from person where age>30").show()
    sparkSession.sql("select * from person where id=3").show()


    sparkSession.stop()
  }
}
