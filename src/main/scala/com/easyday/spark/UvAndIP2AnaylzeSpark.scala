package com.easyday.spark

import java.util.{Random, Date}

import com.easyday.conf.ConfigurationManager
import com.easyday.constract.Constract
import com.easyday.utils.{DateUtil, MockData}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession}

/**
 * Created by admin on 2018/4/3.
 */
object UvAndIP2AnaylzeSpark {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
      .setAppName(Constract.SPARK_APP_NAME)
      .set("spark.shuffle.consolidateFiles","true")
    //      .setMaster("local")
    //      .set("spark.testing.memory","471859200")
    //      .set("spark.sql.warehouse.dir","file:////F://workspace//UV2DongFang")
    //      .set("spark.driver.memory","5g")

    val sc: SparkContext = SparkContext.getOrCreate(conf)
    // sc.setCheckpointDir("/user/zhanghao/data/checkpoint")
    //    sc.setCheckpointDir("E:\\tool\\checkpoint")
    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
//    try {
      //模拟数据
      //mockData(spark)

      val data1 = Nil
      var tmpRDD: RDD[(String, Int)] = sc.parallelize(data1)
      val zeroTime = DateUtil.getZeroTime(new Date()).getTime / 1000
      val dateTime =new Date().getTime
      val time_interval = dateTime / 1000 - zeroTime
      val  random  =new Random()
      var i: Long = 0
      spark.sql(s"use ${ConfigurationManager.getString(Constract.HIVE_DATABASE)}")
      spark.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;")

      for (i <- 1.toLong to time_interval / (ConfigurationManager.getInteger(Constract.TIME_INTERVAL) * 60)) {
        //拉去数据，每十分钟拉一次
        val data: Dataset[Row] = spark.sql(s"select dateline,clientip,qid,uid from " +
          //  s"${ConfigurationManager.getString(Constract.HIVE_DATABASE)}." +
          s"${ConfigurationManager.getString(Constract.HIVE_TABLE)} " +
          s"where dt ='${DateUtil.getTodayDate(new Date())}' " +
          s"and dateline >='${zeroTime + (i - 1) * (ConfigurationManager.getInteger(Constract.TIME_INTERVAL) * 60)}' " +
          s"and dateline < '${zeroTime + i * (ConfigurationManager.getInteger(Constract.TIME_INTERVAL) * 60)}' ")
       // data.show(20)
        val dataRDD = data.rdd

        //将key拼接成uid!qid
        val uidRDDqid: RDD[(String, Int)] = dataRDD.map(
          row => {
            val key = s"${row.getString(3)}!${row.getString(2)}"
            (key, 0)
          }
        )
        //去重
        val groupbyUidQidRDD: RDD[(String, Int)] = uidRDDqid.reduceByKey(_ + _)
        groupbyUidQidRDD.persist(StorageLevel.MEMORY_ONLY)
        tmpRDD = tmpRDD.union(groupbyUidQidRDD)
        // tmpRDD.persist(StorageLevel.MEMORY_ONLY)
      }
      tmpRDD.persist(StorageLevel.MEMORY_ONLY)


      //降数据倾斜聚合 <num+qid,1>
      val key2NumQidRDD :RDD[(String,Long)]= tmpRDD.map(
        row => {
          val QidAndUid = row._1
          val numQid = s"${random.nextInt(9)}#${QidAndUid.split("!")(1)}"
          // val uid = QidAndUid.split("_")(1)
          (numQid, 1.toLong)
        }
      )
      val numQidAggrRDD: RDD[(String, Long)] = key2NumQidRDD.reduceByKey(_ + _)

      //进行聚合<qid,value>
      val key2QidRDD :RDD[(String,Long)]= numQidAggrRDD.map(
        row => {
          val QidAndUid :String = row._1
          val value :Long =row._2
          val qid = QidAndUid.split("#")(1)
         // val uid = QidAndUid.split("_")(1)
          (qid,value)
        }
      )

      val groupBYqidRDD: RDD[(String, Long)] = key2QidRDD.reduceByKey(_ + _)

      println(groupBYqidRDD.take(10))

      //当前时间总UV (String,Long)
      val key2UidRDD :RDD[(String,Long)]=tmpRDD.map(
        row =>{
          val QidAndUid = row._1
          // val qid = QidAndUid.split("_")(0)
          val uid = QidAndUid.split("!")(0)
          (uid, 1.toLong)
        }
      )
      val groupBYuidRDD: RDD[(String, Long)] = key2UidRDD.reduceByKey(_ + _)
      val sum_UV =groupBYuidRDD.count()
      println(s"${sum_UV}    |  date =${dateTime}")
//    }catch{
//      case e :Exception => {
//       e.printStackTrace()
//        System.exit(-1)
//      }
//    }finally{
//
//    }
    sc.stop()
    spark.stop()

  }

  def mockData(spark:SparkSession ): Unit = {
    if (ConfigurationManager.getBoolean(Constract.SPARK_IS_LOCAL)){
      MockData.mock(spark)
    }
  }
}
