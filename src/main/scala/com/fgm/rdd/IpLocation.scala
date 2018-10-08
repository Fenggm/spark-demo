package com.fgm.rdd

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo:利用spark实现ip地址查询
object IpLocation {

    //ip地址转换成Long
    def ipToLong(ip:String):Long={
      val ips = ip.split("\\.")
      var ipNum: Long = 0L
      for (i<-ips){
        ipNum=i.toLong | ipNum << 8L
      }
      ipNum
    }

    def binarySerach(ipNum:Long,array: Array[(String,String,String,String)]):Int={
      var start=0
      var end=array.length-1
      while (start<=end){
        val middle=(start+end)/2

        if (ipNum>=array(middle)._1.toLong&& ipNum<=array(middle)._2.toLong){
          return middle
        }

        if (ipNum<array(middle)._1.toLong){
          end=middle-1
        }

        if (ipNum>array(middle)._2.toLong){
          start=middle+1
        }

      }
      -1
    }

    def dataToMysql(iter:Iterator[((String,String),Int)])={

      //定义数据库连接
      var conn:Connection=null
      var ps:PreparedStatement=null

      val sql="insert into ipLocation(longitude,latitude,total_count) values(?,?,?)"

      conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/spark","root","123")
      ps=conn.prepareStatement(sql)

      try{
        iter.foreach(line => {
          ps.setString(1, line._1._1)
          ps.setString(2, line._1._2)
          ps.setLong(3, line._2)

          ps.execute()
        })
      } catch {
        case e:Exception => println(e)
      } finally {
        if(ps!=null){
          ps.close()
        }

        if(conn!=null){
          conn.close()
        }

      }
    }


    def main(args: Array[String]): Unit = {
      //1、创建SparkConf
      val sparkConf: SparkConf = new SparkConf().setAppName("Iplocation").setMaster("local[2]")

      //2、创建SparkContext
      val sc = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")

      //3、读取城市ip信息日志数据，获取 (ip开始数字、ip结束数字、经度、维度)
      val cityIp = sc.textFile("D:\\tmp\\ip.txt").map(_.split("\\|")).map(x=>(x(2),x(3),x(x.length-2),x(x.length-1)))
      val city_ip_broadcast = sc.broadcast(cityIp.collect())
      //4 读取运营商日志数据，获取所有的ip地址
      val ips = sc.textFile("D:\\tmp\\http.format").map(_.split("\\|")(1))
      //5.遍历ips中每一条数据，获取每一个ip值
      val result:RDD[((String,String),Int)]=ips.mapPartitions(iter=>{
        val array = city_ip_broadcast.value
        //遍历迭代器
        iter.map(ip=>{
          //ip地址转换成long
          val ipNum = ipToLong(ip)
          //把long类型数字通过二分法去数组中匹配，获取对应的下标
          val index = binarySerach(ipNum,array)
          val value = array(index)
          //把结果数据封装在一个元组中
          ((value._3,value._4),1)
        })
      })

      val finalResult = result.reduceByKey(_+_)

      finalResult.foreach(println)

      //把结果数据写入到mysql
      finalResult.foreachPartition(dataToMysql)

      sc.stop()





    }

















}
