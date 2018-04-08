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
    val structType :StructType = StructType(
      Array(StructField("dateline",DataTypes.StringType,true),
        StructField("clientip",DataTypes.StringType,true),
        StructField("qid",DataTypes.StringType,true),
        StructField("uid",DataTypes.StringType,true),
        StructField("dt",DataTypes.StringType,true))
    )

    for( i <- 1 to 100000000){
      val format =new SimpleDateFormat("yyyy-MM-dd 00:00:00")
      val dateline:Long =DateUtil.getZeroTime(new Date()).getTime/1000+random.nextInt(86400)
      val qId = s"qudao${random.nextInt(255)}"

      val clientip =s"${random.nextInt(255)}.${random.nextInt(255)}.${random.nextInt(255)}.${random.nextInt(255)}"
      val uId =s"${random.nextInt(9)}${random.nextInt(9)}${random.nextInt(9)}${random.nextInt(9)}"


      val row = RowFactory.create(String.valueOf(dateline),clientip,uId,qId,dt)
      rows.add(row)
      if (i % 300000 ==0){

        val df :Dataset[Row] =spark.createDataFrame(rows,structType)
        spark.sql("use minieastdaywap")
        df.registerTempTable("log_miniwap_active_realtime1")
        spark.sql("insert into table log_miniwap_active_realtime_source partition(dt='20180408')" +
          " select dateline, clientip,qid,uid from  log_miniwap_active_realtime1 ")
        rows.clear()
      }
    }

  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("1")
      .set("spark.shuffle.consolidateFiles","true")
//      .setMaster("local")
//      .set("spark.testing.memory","471859200")
//      .set("spark.sql.warehouse.dir","file:////F://workspace//UV2DongFang")
//      .set("spark.driver.memory","5g")


    val sc =SparkContext.getOrCreate(conf)
    val spark =SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    mock(spark)

  }
}
