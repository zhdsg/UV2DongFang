package com.easyday.utils

import java.text.SimpleDateFormat
import java.util
import java.util.{UUID, Random, Date}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession, RowFactory, Row}

import scala.collection.mutable

/**
 * Created by admin on 2018/4/3.
 */
object MockData {


  def mock(spark : SparkSession): Unit ={
    val rows= new util.ArrayList[Row]()

    val dt = DateUtil.getTodayDate()
    var i = 0
    val random = new Random()
    for( i <- 1 to 10000){
      val format =new SimpleDateFormat("yyyy-MM-dd 00:00:00")
      val dateline:Long =format.parse(format.format(new Date())).getTime+random.nextInt(86400*1000)
      val qId = s"qudao${random.nextInt(255)}"

      val clientip =s"${random.nextInt(255)}.${random.nextInt(255)}.${random.nextInt(255)}.${random.nextInt(255)}"
      val uId =UUID.randomUUID().toString.replace("-","")


      val row = RowFactory.create(String.valueOf(dateline),clientip,uId,qId,dt)
      rows.add(row)
    }

    val structType :StructType = StructType(
      Array(StructField("dateline",DataTypes.StringType,true),
            StructField("clientip",DataTypes.StringType,true),
            StructField("qid",DataTypes.StringType,true),
            StructField("uid",DataTypes.StringType,true),
            StructField("dt",DataTypes.StringType,true))
    )

    val df :Dataset[Row] =spark.createDataFrame(rows,structType)
    df.registerTempTable("log_miniwap_active_realtime")
    df.show(100)

  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("1")
      .setMaster("local")
      .set("spark.testing.memory","471859200")
      .set("spark.sql.warehouse.dir","file:////F://workspace//UV2DongFang")
      .set("spark.driver.memory","5g")


    val sc =SparkContext.getOrCreate(conf)
    val spark =SparkSession.builder().config(conf).getOrCreate()

    mock(spark)

  }
}
