package com.easyday.spark

import com.easyday.conf.ConfigurationManager
import com.easyday.constract.Constract
import com.easyday.utils.{DateUtil, MockData}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, Dataset, SparkSession}

/**
 * Created by admin on 2018/4/3.
 */
object UvAndIP2AnaylzeSpark {

  def main(args: Array[String]) {

    val conf : SparkConf = new SparkConf()
      .setAppName(Constract.SPARK_APP_NAME)
//      .setMaster("local")
//      .set("spark.testing.memory","471859200")
//      .set("spark.sql.warehouse.dir","file:////F://workspace//UV2DongFang")
//      .set("spark.driver.memory","5g")

    val sc: SparkContext= SparkContext.getOrCreate(conf)
    sc.setCheckpointDir("/user/zhanghao/data/checkpoint")
//    sc.setCheckpointDir("E:\\tool\\checkpoint")
    val spark:SparkSession=SparkSession.builder()
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
    import spark.implicits._


    mockData(spark)


    val data :Dataset[Row]=spark.sql(s"select dateline,clientip,qid,uid from " +
      s"${ConfigurationManager.getString(Constract.HIVE_DATABASE)}." +
      s"${ConfigurationManager.getString(Constract.HIVE_TABLE)} " +
      s"where dt ='${DateUtil.getTodayDate()}'")
    data.show(100)
    val dataRDD = data.rdd

    val uidRDDqid:RDD[(String,String)] =dataRDD.map(
      row => {
        val key = s"${row.getString(2)}_${row.getString(3)}"
        (key,key)
      }
    )
    val groupbyUidQidRDD =uidRDDqid.groupByKey()

    val key2QidRDD =groupbyUidQidRDD.map(
      row =>{
        val QidAndUid = row._1
        val qid =QidAndUid.split("_")(0)
        val uid =QidAndUid.split("_")(1)
        (qid,uid)
      }
    )

    val  groupBYqidRDD:RDD[(String,Iterable[String])] = key2QidRDD.groupByKey()
    val resultRDD =groupBYqidRDD.map(
      row => {
        var sum = 0
        val qid =row._1
        val iterator:Iterator[String] = row._2.iterator
        while (iterator.hasNext){
          sum=sum+1
        }
        (qid,sum)
      }

    )

//    data.rdd.checkpoint()

    resultRDD.collect().foreach(row=>println(row._1 +":" +row._2))




  }

  def mockData(spark:SparkSession ): Unit = {
    if (ConfigurationManager.getBoolean(Constract.SPARK_IS_LOCAL)){
      MockData.mock(spark)
    }
  }
}
