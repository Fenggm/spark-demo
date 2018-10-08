package com.fgm.sparksql

import org.apache.spark.sql.SparkSession

/**
  *SparkSql操作
  *
  * @Auther: fgm
  */
object HiveSparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveSparkSql").master("local[2]").enableHiveSupport().getOrCreate()
    spark.sql("create table user(id int,name string,age int) row format delimited fields terminated by ','")
    spark.sql("load data local inpath './data/user.txt' into table user")
    spark.sql("select * from user").show()

    spark.stop()
  }

}
