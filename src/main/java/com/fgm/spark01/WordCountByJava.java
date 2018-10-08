package com.fgm.spark01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @Auther: fgm
 */
public class WordCountByJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCount_java").setMaster("local[2]");
        //创建javaSparkContext对象
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //读取数据文件
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("D:\\a1.txt");


        //切分
        JavaRDD<String> wordsJavaRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        //每个单词记为1
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = wordsJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        
        //相同单词出现的1累加
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD2 = stringIntegerJavaPairRDD1.sortByKey();

        List<Tuple2<String, Integer>> collect = stringIntegerJavaPairRDD2.collect();

        for (Tuple2<String, Integer> t : collect) {
            System.out.println("单词："+t._1+" 次数："+t._2);
        }
    }
}
