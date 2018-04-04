package com.easyday.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by admin on 2018/4/4.
 */
object WordCountDemo {
  def main(args: Array[String]) {

    val conf :SparkConf =new SparkConf().setAppName("wordCount")
//      .set("spark.testing.memory","471859200")
//      .setMaster("local")
    val sc :SparkContext = SparkContext.getOrCreate(conf)
    val spark :SparkSession =SparkSession.builder().config(conf).getOrCreate()
    val demo = sc.parallelize(Array(s"sss sss asda wsc",s"sss sss asda wsc",s"sss sss asda wsc",s"sss sss asda wsc"))

    demo.flatMap(line => line.split(" ")).map((_,1)).reduceByKey(_ + _ ).collect().foreach(x => println(x._1+" ："+ x._2))

  }

}
