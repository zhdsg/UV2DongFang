package com.easyday.spark

import java.util.{Date, Random}


import cn.eastday.dao.{UvAggrDao, MiddleDao}
import com.easyday.conf.ConfigurationManager
import com.easyday.constract.Constract
import com.easyday.dao.DAOFactory

import com.easyday.domain.LogRecord
import com.easyday.utils.{ETLUtil, DataUtil, DateUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by admin on 2018/4/9.
 */
object UvAndIP2AnaylzeSpark1 {
   def main (args: Array[String]){
   // val logger :Logger = Logger.getLogger(UvAndIP2AnaylzeSpark2.getClass)
     if(args.length==0){
        println("please input params........")
       System.exit(-1)
     }
     val dateStr :StringBuffer=new StringBuffer("")
       dateStr.append(DateUtil.getFormatTime(args(0)))


    //spark 具体句柄创建
     val conf: SparkConf = new SparkConf()
       .setAppName(Constract.SPARK_APP_NAME)
       .set(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES
            ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES))
       .set(Constract.SPARK_SHUFFLE_FILE_BUFFER
            ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_FILE_BUFFER))
       .set(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT
            ,ConfigurationManager.getString(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT))
       .set(Constract.SPARK_SHUFFLE_IO_MAXRETRIES
            ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_IO_MAXRETRIES))
       .set(Constract.SPARK_DEFAULT_PARALLELISM
            ,ConfigurationManager.getString(Constract.SPARK_DEFAULT_PARALLELISM))
      // .set("spark.shuffle.manager","hash")
        //.set("spark.shuffle.sort.bypassMergeThreshold","550")
        //.set("spark.shuffle.memoryFraction","0.2")
        //.set("spark.storage.memoryFraction","0.5")

     //      .setMaster("local")
     //      .set("spark.testing.memory","471859200")
     //      .set("spark.sql.warehouse.dir","file:////F://workspace//UV2DongFang")
     //      .set("spark.driver.memory","5g")

     //val sc: SparkContext = SparkContext.getOrCreate(conf)
     val sc =new SparkContext(conf)
     //设置读取时长
     sc.hadoopConfiguration.set(Constract.DFS_CLIENT_SOCKET_TIMEOUT
       ,ConfigurationManager.getString(Constract.DFS_CLIENT_SOCKET_TIMEOUT))
     // sc.setCheckpointDir("/user/zhanghao/data/checkpoint")
     //    sc.setCheckpointDir("E:\\tool\\checkpoint")
     val hiveContext =new HiveContext(sc)
     hiveContext.setConf("hive.metastore.warehouse.dir","hdfs://Ucluster/user/hive/warehouse")
//     val spark: SparkSession = SparkSession.builder()
//       .config(conf)
//       .enableHiveSupport()
//       .getOrCreate()
     // val broadcast :Broadcast[String] =sc.broadcast(dateStr.toString)
    //  logger.debug(s"spark & sparkContext  inited")
     val random =new Random()
     val upLine :Long=DateUtil.str2Date(DateUtil.trimDate(dateStr.toString)).getTime/1000
     val downLine:Long =upLine-600

     val dateTime =DateUtil.trimDate(dateStr.toString)
     val dateLineUp =upLine
     var dateLineDown=DateUtil.getZeroTime(dateStr.toString()).getTime/1000
     var dt =DateUtil.getTodayDate(dateStr.toString)
     if (dateLineDown ==dateLineUp){
       dateLineDown =dateLineDown-86400
       dt =DateUtil.getYesterdayDate(dateStr.toString)
     }
      //val dateTime =new Date().getTime
     try {
       //模拟数据
       //mockData(spark)

       //数据格式：RDD[(Row:[dateline,clientip,qid,uid])]
        var dataRDD:RDD[Row]=getData(hiveContext,dt,dateLineDown,dateLineUp)
        dataRDD =dataRDD.persist(StorageLevel.MEMORY_ONLY)
        dataRDD.first()


      //  println(s"${DateUtil.date2Str(new Date())}: dataRDD has been loaded and perist.....")

        //数据去重<uid!qid>
         var groupbyUidQidRDD: RDD[(String, Long)] = distinctByUIdAndQId(dataRDD)

        groupbyUidQidRDD=groupbyUidQidRDD.persist(StorageLevel.MEMORY_ONLY)
        //数据去重<IP!qid> job
        var groupbyIpQidRDD: RDD[(String, Long)] = distinctByIpAndQId(dataRDD)
        groupbyIpQidRDD =groupbyIpQidRDD.persist(StorageLevel.MEMORY_ONLY)
        //获取前十分钟数据
        var dataTenMinRDD =dataRDD.filter(
          row=>{
            val dateline:Long =row.getLong(0)
            if(dateline <= upLine && dateline >downLine  ){
              true
            }else{
              false
            }

          }
        ).repartition(100)

        //公共RDD持久化
        dataTenMinRDD=dataTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
        //println(s"${DateUtil.date2Str(new Date())}: tenMinRDD has been peristed ...........")

        //val total_PV =dataRDD.count()
        // val total_PV_10m =dataTenMinRDD.count()
        //val groupBYqid4PvTenMinRDD:RDD[(String,Long)] =getAggr2qid4PvRDD(dataTenMinRDD,random)
        //val groupBYqid4PvRDD:RDD[(String,Long)] =getAggr2qid4PvRDD(dataRDD,random)

        //聚合distinct
        var groupByUidQidTenMinRDD: RDD[(String, Long)]= distinctByUIdAndQId(dataTenMinRDD)
        dataRDD=dataRDD.unpersist()
        groupByUidQidTenMinRDD=groupByUidQidTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
       // println(s"${DateUtil.date2Str(new Date())}: groupByUidQidTenMinRDD has been peristed ...........")
        var groupByIpQidTenMinRDD: RDD[(String, Long)]= distinctByIpAndQId(dataTenMinRDD)
        groupByIpQidTenMinRDD=groupByIpQidTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
      //  println(s"${DateUtil.date2Str(new Date())}: groupByIpQidTenMinRDD has been peristed ...........")

        //企业id聚合的UV
        val groupBYqid4UvRDD:RDD[(String,Long)]=getAggr2qidRDD(groupbyUidQidRDD,random)
        val groupBYqid4UvTenMinRDD:RDD[(String,Long)]=getAggr2qidRDD(groupByUidQidTenMinRDD,random)

        //IP
        val groupBYqid4IpRDD:RDD[(String,Long)]=getAggr2qidRDD(groupbyIpQidRDD,random)
        val groupBYqid4IpTenMinRDD:RDD[(String,Long)]=getAggr2qidRDD(groupByIpQidTenMinRDD,random)

        //PV
        var groupBYqid4PvRDD:RDD[(String,Long)] =getAggr2qid4PvRDD(groupbyUidQidRDD)
        groupBYqid4PvRDD=groupBYqid4PvRDD.persist(StorageLevel.MEMORY_ONLY)
        var groupBYqid4PvTenMinRDD:RDD[(String,Long)] =getAggr2qid4PvRDD(groupByUidQidTenMinRDD)
        groupBYqid4PvTenMinRDD=groupBYqid4PvTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
        //println(s"${DateUtil.date2Str(new Date())} qid4PvRDD has been computed ...........")
        //logger.info(s"${DateUtil.date2Str(new Date())} qid4PvRDD has been peristed ...........")



        //总UV
        val total_UV =getAggr2SumUV(groupbyUidQidRDD)
        val total_UV_10m=getAggr2SumUV(groupByUidQidTenMinRDD)
        val total_IP=getAggr2SumIP(groupbyIpQidRDD)
        val total_IP_10m=getAggr2SumIP(groupByIpQidTenMinRDD)
        val total_PV =groupBYqid4PvRDD.map(row =>(row._2)).reduce(_ + _)
        val total_PV_10m =groupBYqid4PvTenMinRDD.map(row=>(row._2)).reduce(_ + _)

        println(s"${DateUtil.date2Str(new Date())} prepare  join ...........")
        val resultRDD :RDD[(LogRecord)] =groupBYqid4PvRDD.leftOuterJoin(groupBYqid4UvRDD)
          .leftOuterJoin(groupBYqid4IpRDD).leftOuterJoin(groupBYqid4PvTenMinRDD)
          .leftOuterJoin(groupBYqid4UvTenMinRDD).leftOuterJoin(groupBYqid4IpTenMinRDD)
          .map(
            row =>{
              val qid = if (row._1.length >50) {row._1.substring(0,50)}else{row._1}
              val pv :Long=row._2._1._1._1._1._1
              val uv :Long =DataUtil.someOrNone(row._2._1._1._1._1._2,0)
              val ip :Long= DataUtil.someOrNone(row._2._1._1._1._2 ,0)
              val incr_pv:Long = DataUtil.someOrNone(row._2._1._1._2,0)
              val incr_uv :Long= DataUtil.someOrNone(row._2._1._2 ,0)
              val incr_ip:Long = DataUtil.someOrNone(row._2._2,0)
              LogRecord(dt.toInt,dateLineUp,qid,pv,uv,ip,incr_pv,incr_uv,incr_ip)
            }
          )

//        resultRDD.foreachPartition(partition =>{
//         val uvdao :UvAggrDaoImpl =DAOFactory.getUvAggrDao()
//            while(partition.hasNext){
//              val row =partition.next()
//              uvdao.insert(row)
//            }
//        })
        val uvDao =DAOFactory.getUvAggrDao()
        val tableName =ConfigurationManager.getString(Constract.H5_TABLE_NAME)
        val tableName2=ConfigurationManager.getString(Constract.APP_TABLE_NAME2)
        uvDao.delete(dt.toInt,dateLineUp,tableName)
        uvDao.insert(LogRecord(dt.toInt,dateLineUp,"total",total_PV,total_UV,total_IP,total_PV_10m,total_UV_10m,total_IP_10m),tableName)


        resultRDD.collect.foreach(row =>{
          val uvdao :UvAggrDao =DAOFactory.getUvAggrDao()
          uvdao.insert(row,tableName)
        })

        val midDao :MiddleDao=DAOFactory.getMiddleDao()
        midDao.getDateInsert(dt.toInt,dateLineUp,tableName,tableName2)


      //打印信息========================================================================
//        val row =Nil
//        for( row  <- dataRDD.map(row=>(row.getLong(0),1)).sortByKey(false).take(1)){
//            println(row)
//          }
//        for( row  <- groupBYqidRDD.take(10)){
//          println(row)
//        }
         println(s"${DateUtil.date2Str(new Date())}|uv_sum:${total_UV}  |ip_sum ${total_IP} |incr_uv:${total_UV_10m} |incr_ip:${total_IP_10m} |pv_sum:${total_PV}  |incr_pv:${total_PV_10m}")

       //打印信息=======================================================================
     }catch{
       case e :Exception => {
        e.printStackTrace()

//         sc.stop()
//         System.exit(-1)

       }
     }finally{
       sc.stop()

     }

   }

  /**
   *
   * @param hiveContext
   * @param dt
   * @param dateLineDown
   * @param dateLineUp
   * @return
   */
  def getData(hiveContext:HiveContext  ,dt:String,dateLineDown:Long,dateLineUp:Long):RDD[(Row)]={


    hiveContext.setConf("hive.merge.mapfiles","true")
    hiveContext.setConf("mapred.max.split.size","536870912")
    hiveContext.setConf("mapred.min.split.size.per.node","536870912")
    hiveContext.setConf("mapred.min.split.size.per.rack","536870912")
    hiveContext.setConf("hive.input.format","org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    val strBuild =new StringBuilder("")
    hiveContext.sql(s"use ${ConfigurationManager.getString(Constract.HIVE_DATABASE)}")
//    spark.sql("set mapred.max.split.size= 536870912;")
//    spark.sql("set mapred.min.split.size.per.node= 536870912")
//    spark.sql("set mapred.min.split.size.per.rack= 536870912")
//    spark.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;")
//    val sql =
//      s"""
//         |select dateline,clientip,qid,
//         |case when qid='huitt' then clientime else uid end as uid  from
//         |${ConfigurationManager.getString(Constract.HIVE_TABLE)}
//         |where dt ='${dt}' and
//         |( ((qid='null'   or qid is null)
//         |AND UPPER(apptypeid) NOT in ('DFTT','TTKB')
//         |AND UPPER(apptypeid) NOT REGEXP '^ZJZY|^GSIOS'
//         |AND UPPER(apptypeid) NOT IN (
//         |'XWZXTT','XNTTZK','JSSTT','JS_DFTT','USATT','HBJTT','MOPSHIPIN',
//         |'YZSTT','DBHTT','GSGTT','NCJTT','GZGTT','YNYTT','ICANTT','JXGTT',
//         |'HNYTT','HNQTT','XNBLW','XGGTT','GDYTT','TQYB','TQKB','WNWB','GSBROWSER'
//         | ) ) OR (qid<>'null'   AND qid is NOT null) )
//         | and dateline <='${dateLineUp}' and dateline >='${dateLineDown}
//       """.stripMargin
    strBuild.append(
      s"select dateline,clientip,qid, " +
        s"case when qid='huitt' then clientime else uid end as uid  from " +
      //  s"${ConfigurationManager.getString(Constract.HIVE_DATABASE)}." +
      s"${ConfigurationManager.getString(Constract.HIVE_TABLE)} " +
      s"where dt ='${dt}' "
        +
      s"and  ( ((qid='null'   or qid is null) " +
      s"AND UPPER(apptypeid) NOT in ('DFTT','TTKB') " +
      s"AND UPPER(apptypeid) NOT REGEXP '^ZJZY|^GSIOS' " +
      s"AND UPPER(apptypeid) NOT IN ( " +
      s"'XWZXTT','XNTTZK','JSSTT','JS_DFTT','USATT','HBJTT','MOPSHIPIN','YZSTT',"+
      s"'DBHTT','GSGTT','NCJTT','GZGTT','YNYTT','ICANTT','JXGTT','HNYTT', " +
      s"'HNQTT','XNBLW','XGGTT','GDYTT','TQYB','TQKB','WNWB','GSBROWSER'" +
      s") ) OR (qid<>'null'   AND qid is NOT null) )"
      )



      strBuild.append(s"and dateline <='${dateLineUp}' " +
        s"and dateline >='${dateLineDown}'")

    val sql =strBuild.toString()
    println(sql)
    val data: DataFrame = hiveContext.sql(sql)
    val  dataRDD =data.rdd.coalesce(300)
    dataRDD
//    val datafilter = data.rdd.filter(
//    row =>{
//      val qid =row.getString(3)
//      if( qid ==null||qid.equals("null")) {
//        val apptypeid=row.getString(5)
//        if(apptypeid==null){
//          true
//        }else {
//          if(apptypeid.length>=4&&apptypeid.substring(0,4).equals("ZJZY")){
//            false
//          }else if (apptypeid.length>=5&&apptypeid.substring(0,5).equals("GSIOS")){
//            false
//          } else if(apptypeid.equals("DFTT")||apptypeid.equals("TTKB")||apptypeid.equals("XWZXTT")||apptypeid.equals("XNTTZK")
//            ||apptypeid.equals("JSSTT")||apptypeid.equals("JS_DFTT")||apptypeid.equals("USATT")||apptypeid.equals("HBJTT")
//            ||apptypeid.equals("MOPSHIPIN")||apptypeid.equals("YZSTT")||apptypeid.equals("DBHTT")||apptypeid.equals("GSGTT")
//            ||apptypeid.equals("NCJTT")||apptypeid.equals("GZGTT")||apptypeid.equals("YNYTT")||apptypeid.equals("ICANTT")
//            ||apptypeid.equals("JXGTT")||apptypeid.equals("HNYTT")||apptypeid.equals("HNQTT")||apptypeid.equals("XNBLW")
//            ||apptypeid.equals("GDYTT")||apptypeid.equals("TQYB")||apptypeid.equals("TQKB")||apptypeid.equals("WNWB")
//            ||apptypeid.equals("GSBROWSER")||apptypeid.equals("XGGTT")){
//            false
//          }else{
//            true
//          }
//        }
//      }else{
//        true
//      }
//
//    }
//    ).coalesce(300)
////  .mapPartitions(
////      iterator=>{
////        var res =List[(Row)]()
////        while(iterator.hasNext){
////          val cur =iterator.next()
////          val dateline=cur.getLong(0)
////          val clientip=cur.getString(1)
////          val qid=cur.getString(2)
////          var uid =cur.getString(3)
////          val clientime =cur.getString(4)
////          if(qid.toLowerCase().equals("huitt")){
////            uid =clientime
////          }
////          val row :Row =Row(dateline,clientip,qid,uid)
////          res ::=row
////        }
////        res.iterator
////      }
////    )
//      .map(
//        cur => {
//              val dateline=cur.getLong(0)
//              val clientip=cur.getString(1)
//              val qid=cur.getString(2)
//              var uid =cur.getString(3)
//              val clientime =cur.getString(4)
//              if(qid.toLowerCase().equals("huitt")){
//                uid =clientime
//              }
//              Row(dateline,clientip,qid,uid)
//        }
//      )
//    datafilter
  }



  /**
   * 数据去重< uid!qid ,0>
   * @param dataRDD
   * @return
   */
  def distinctByUIdAndQId(dataRDD:RDD[(Row)]):RDD[(String,Long)]={
    val uidRDDqid: RDD[(String, Long)] = dataRDD.map(
      row => {
        val key = s"${row.getString(3)}!${ETLUtil.trimSpecial(row.getString(2).toLowerCase())}"

        (key,1.toLong)
      }
    )
    val groupbyUidQidRDD: RDD[(String, Long)] = uidRDDqid.reduceByKey(_ + _)
    groupbyUidQidRDD
  }

  /**
   *
   * @param dataRDD
   * @return
   */
  def distinctByIpAndQId(dataRDD:RDD[(Row)]):RDD[(String,Long)]={
    val ipRDDqid: RDD[(String, Long)] = dataRDD.map(
      row => {
        (s"${row.getString(1)}!${ETLUtil.trimSpecial(row.getString(2).toLowerCase())}",1.toLong)
      }
    )
    val groupbyIpQidRDD: RDD[(String, Long)] = ipRDDqid.reduceByKey(_ + _)
    groupbyIpQidRDD
  }

  /**
   * 根据企业ID:qid进行聚合统计
   * @param groupbyUidQidRDD 已经根据uid!qid去重后的RDD
   * @param random 随机数，用于降数据倾斜
   * @return
   */
  def getAggr2qidRDD(groupbyUidQidRDD:RDD[(String,Long)],random:Random):RDD[(String,Long)]= {
    //降数据倾斜聚合 <num+qid,1>
    val key2NumQidRDD: RDD[(String, Long)] = groupbyUidQidRDD.map(
      row => {
        val QidAndUid = row._1
        val numQid = s"${random.nextInt(9)}#${QidAndUid.split("!")(1)}"
        // val uid = QidAndUid.split("_")(1)
        (numQid, 1.toLong)
      }
    )
    val numQidAggrRDD: RDD[(String, Long)] = key2NumQidRDD.reduceByKey(_ + _)

    //将第一遍统计的数据在聚合
    val key2QidRDD: RDD[(String, Long)] = numQidAggrRDD.map(
      row => {
        val numQid = row._1
        val value = row._2
        val qid = numQid.split("#")(1)
        // val uid = QidAndUid.split("_")(1)
        (qid, value)
      }
    )

    val groupBYqidRDD: RDD[(String, Long)] = key2QidRDD.reduceByKey(_ + _)
    groupBYqidRDD
  }

  /**
   * 根据企业ID:qid进行聚合统计
   * @param groupbyUidQidRDD
   * @return
   */
  def getAggr2qid4PvRDD(groupbyUidQidRDD:RDD[(String,Long)]):RDD[(String,Long)]={
    //降数据倾斜聚合 <num+qid,1>
    val key2NumQidRDD: RDD[(String, Long)] = groupbyUidQidRDD.map(
      row => {
        val qid =row._1.split("!")(1)
        val value =row._2
        (qid, value)
      }
    )
   val resultRDD: RDD[(String, Long)] = key2NumQidRDD.reduceByKey(_ + _)
    resultRDD
  }
  /**
   * 获取总UV
   * @param groupbyUidQidRDD  已经根据uid!qid去重后的RDD
   * @return
   */
  def getAggr2SumUV(groupbyUidQidRDD:RDD[(String,Long)]) :Long ={
    //当前时间总UV
    val key2UidRDD :RDD[(String ,Long)]=groupbyUidQidRDD.map(
      row =>{
        val QidAndUid = row._1
        val uid = QidAndUid.split("!")(0)
        (uid,1.toLong)
      }
    )
    val groupBYuidRDD: RDD[(String, Long)] = key2UidRDD.reduceByKey(_ + _)
    groupBYuidRDD.count()
  }

  /**
   * 获取总IP
   * @param groupbyIpQidRDD
   * @return
   */
  def getAggr2SumIP(groupbyIpQidRDD:RDD[(String,Long)]):Long ={
    //当前时间总UV
    val key2IpRDD :RDD[(String ,Long)]=groupbyIpQidRDD.map(
      row =>{
        val ipAndQid = row._1
        // val qid = QidAndUid.split("_")(0)
        val ip = ipAndQid.split("!")(0)
        (ip,1.toLong)
      }
    )
    val groupByIpRDD: RDD[(String, Long)] = key2IpRDD.reduceByKey(_ + _)
    groupByIpRDD.count()
  }


}
