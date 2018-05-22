package com.easyday.spark

import java.util
import java.util.{Random, Date}

import cn.eastday.dao.UvAggrDao
import com.easyday.conf.ConfigurationManager
import com.easyday.constract.Constract
import com.easyday.dao.DAOFactory
import com.easyday.domain.LogRecord
import com.easyday.utils.{ETLUtil, DataUtil, DateUtil}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{RowFactory, Row, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by admin on 2018/4/9.
 */
object UvAndIP2AnaylzeSpark2 {
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


     val sc: SparkContext = SparkContext.getOrCreate(conf)
     //设置读取时长
     sc.hadoopConfiguration.set(Constract.DFS_CLIENT_SOCKET_TIMEOUT
       ,ConfigurationManager.getString(Constract.DFS_CLIENT_SOCKET_TIMEOUT))

     val spark: SparkSession = SparkSession.builder()
       .config(conf)
       .enableHiveSupport()
       .getOrCreate()
     import spark.implicits._
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

       //import scala.collection.JavaConversions._
       val list: util.ArrayList[String] = DAOFactory.getUvAggrDao().getSDKqid()


       //数据格式：RDD[(Row:[dateline,clientip,qid,uid])]
        var dataRDD:RDD[Row]=getData(spark,dt,dateLineDown,dateLineUp)
        //dataRDD =dataRDD.persist(StorageLevel.MEMORY_ONLY)
        val sdkRDD = dataRDD.filter(f=>{
          var flag =false
          val qid  =f.getString(2)

          for( i <- 0 to  list.size()-1){
            if(qid.equals(list.get(i))){
              flag = true
            }
          }
          flag
        }).map(f => Row(f.getLong(0),f.getString(1),"sdk_total",f.getString(3)))
        val unionRDD =dataRDD.union(sdkRDD)
         unionRDD.persist()
         unionRDD.first()
        //数据去重<uid!qid>
         var groupbyUidQidRDD: RDD[((String,String), Long)] = distinctByUIdAndQId(unionRDD)

        groupbyUidQidRDD=groupbyUidQidRDD.persist(StorageLevel.MEMORY_ONLY)
        //数据去重<IP!qid> job
        var groupbyIpQidRDD: RDD[((String,String), Long)] = distinctByIpAndQId(unionRDD)
        groupbyIpQidRDD =groupbyIpQidRDD.persist(StorageLevel.MEMORY_ONLY)
        //获取前十分钟数据
        var dataTenMinRDD =unionRDD.filter(
          row=>{
            val dateline:Long =row.getLong(0)
            if(dateline <= upLine && dateline >downLine  ){
              true
            }else{
              false
            }
          }
        )

        //公共RDD持久化
        dataTenMinRDD=dataTenMinRDD.persist(StorageLevel.MEMORY_ONLY)

        //聚合distinct
        var groupByUidQidTenMinRDD: RDD[((String,String), Long)]= distinctByUIdAndQId(dataTenMinRDD)

        groupByUidQidTenMinRDD=groupByUidQidTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
       // println(s"${DateUtil.date2Str(new Date())}: groupByUidQidTenMinRDD has been peristed ...........")
        var groupByIpQidTenMinRDD: RDD[((String,String), Long)]= distinctByIpAndQId(dataTenMinRDD)
        groupByIpQidTenMinRDD=groupByIpQidTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
      //  println(s"${DateUtil.date2Str(new Date())}: groupByIpQidTenMinRDD has been peristed ...........")

        //企业id聚合的UV
        val groupBYqid4UvRDD:RDD[(String,Long)]=getAggr2qidRDD(groupbyUidQidRDD)
        val groupBYqid4UvTenMinRDD:RDD[(String,Long)]=getAggr2qidRDD(groupByUidQidTenMinRDD)

        //IP
        val groupBYqid4IpRDD:RDD[(String,Long)]=getAggr2qidRDD(groupbyIpQidRDD)
        val groupBYqid4IpTenMinRDD:RDD[(String,Long)]=getAggr2qidRDD(groupByIpQidTenMinRDD)

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
        dataRDD=dataRDD.unpersist()

       //==============================active==============
       val activeRDD4Active =getDataForActive(spark,dt,dateLineDown,dateLineUp)
       val sdkRDD4Active = dataRDD.filter(f=>{
         var flag =false
         val qid  =f.getString(2)
         for( i <- 0 to  list.size()-1){
           if(qid.equals(list.get(i))){
             flag = true
           }
         }
         flag
       }).map(f => Row(f.getLong(0),f.getString(1),"sdk_total",f.getString(3)))
       val unionRDD4Active =activeRDD4Active.union(sdkRDD4Active)
       unionRDD4Active.persist()
       unionRDD4Active.first()
       //十分钟active
       val dataTenMinRDD4Active =unionRDD4Active.filter(
         row=>{
           val dateline:Long =row.getLong(0)
           if(dateline <= upLine && dateline >downLine  ){
             true
           }else{
             false
           }
         }
       )
       val groupbyUidQidRDD4Active: RDD[((String,String), Long)] = distinctByUIdAndQId(unionRDD4Active)
       groupbyUidQidRDD4Active.persist()
       val groupByUidQidTenMinRDD4Active: RDD[((String,String), Long)]= distinctByUIdAndQId(dataTenMinRDD4Active)
       groupByUidQidTenMinRDD4Active.persist()
       val activeRDD:RDD[(String,Long)]=getAggr2qidRDD(groupbyUidQidRDD4Active)
       val activePre10minRDD:RDD[(String,Long)]=getAggr2qidRDD(groupByUidQidTenMinRDD4Active)
       val total_UV_active =getAggr2SumUV(groupbyUidQidRDD4Active)
       val total_UV_10m_active=getAggr2SumUV(groupByUidQidTenMinRDD4Active)

       //==============================================

        println(s"${DateUtil.date2Str(new Date())} prepare  join ...........")
        val resultRDD :RDD[(LogRecord)] =groupBYqid4PvRDD.leftOuterJoin(groupBYqid4UvRDD)
          .leftOuterJoin(groupBYqid4IpRDD).leftOuterJoin(groupBYqid4PvTenMinRDD)
          .leftOuterJoin(groupBYqid4UvTenMinRDD).leftOuterJoin(groupBYqid4IpTenMinRDD)
          .leftOuterJoin(activeRDD).leftOuterJoin(activePre10minRDD)
          .map {
            case (qID,(((((((pV,uV),iP),incr_PV),incr_UV),incr_IP),activeCNT),incr_ACTIVECNT)) => {
              val qid = if (qID.length > 50) {
                qID.substring(0, 50)
              } else {
                qID
              }
              val pv: Long = pV
              val uv: Long = DataUtil.someOrNone(uV, 0)
              val ip: Long = DataUtil.someOrNone(iP, 0)
              val incr_pv: Long = DataUtil.someOrNone(incr_PV, 0)
              val incr_uv: Long = DataUtil.someOrNone(incr_UV, 0)
              val incr_ip: Long = DataUtil.someOrNone(incr_IP, 0)
              val activecnt =DataUtil.someOrNone(activeCNT,0)
              val incr_activecnt=DataUtil.someOrNone(incr_ACTIVECNT,0)
              LogRecord(dt.toInt, dateLineUp, qid, pv, uv, ip
                ,incr_pv, incr_uv, incr_ip,activecnt,incr_activecnt)
            }
          }

//        resultRDD.foreachPartition(partition =>{
//         val uvdao :UvAggrDaoImpl =DAOFactory.getUvAggrDao()
//            while(partition.hasNext){
//              val row =partition.next()
//              uvdao.insert(row)
//            }
//        })


        val uvDao =DAOFactory.getUvAggrDao()
        val tableName =ConfigurationManager.getString(Constract.H5_TABLE_NAME)
        val tableName2 =ConfigurationManager.getString(Constract.H5_TABLE_NAME2)
        uvDao.delete(dt.toInt,dateLineUp,tableName)
        uvDao.insert(
          LogRecord(dt.toInt,dateLineUp,"total",total_PV,total_UV,total_IP
            ,total_PV_10m,total_UV_10m,total_IP_10m,total_UV_active,total_UV_10m_active),tableName)


        resultRDD.collect().foreach(
          row=>{
            val uvdao :UvAggrDao =DAOFactory.getUvAggrDao()
             uvdao.insert(row,tableName)

          })

        val midDao =DAOFactory.getMiddleDao()
        midDao.getDateInsert(dt.toInt,dateLineUp,tableName,tableName2)



         println(s"${DateUtil.date2Str(new Date())}|uv_sum:${total_UV}  |ip_sum ${total_IP}" +
           s" |incr_uv:${total_UV_10m} |incr_ip:${total_IP_10m} |pv_sum:${total_PV}  |incr_pv:${total_PV_10m}" +
           s" |activecnt:${total_UV_active} |incr_activecnt:${total_UV_10m_active}")

       //打印信息=======================================================================
     }catch{
       case e :Exception => {
        e.printStackTrace()
         sc.stop()
         spark.stop()
         System.exit(-1)

       }
     }finally{
       sc.stop()
       spark.stop()
     }

   }

  /**
   *
   * @param spark
   * @param dt
   * @param dateLineDown
   * @param dateLineUp
   * @return
   */
  def getData(spark:SparkSession  ,dt:String,dateLineDown:Long,dateLineUp:Long):RDD[(Row)]={


    spark.sqlContext.setConf("hive.merge.mapfiles","true")
    spark.sqlContext.setConf("mapred.max.split.size","536870912")
    spark.sqlContext.setConf("mapred.min.split.size.per.node","536870912")
    spark.sqlContext.setConf("mapred.min.split.size.per.rack","536870912")
    spark.sqlContext.setConf("hive.input.format","org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    val strBuild =new StringBuilder("")
    spark.sql(s"use ${ConfigurationManager.getString(Constract.HIVE_DATABASE)}")

    strBuild.append(
      s"select dateline,clientip,qid, " +
      s"uid ,apptypeid ,clientime "+
    //  s"case when qid='huitt' then clientime else uid end as uid  " +
      s"from " +
      //  s"${ConfigurationManager.getString(Constract.HIVE_DATABASE)}." +
      s"${ConfigurationManager.getString(Constract.HIVE_TABLE)} " +
      s"where dt ='${dt}' "
//        +
//      s"and  ( ((qid='null'   or qid is null) " +
//      s"AND UPPER(apptypeid) NOT in ('DFTT','TTKB') " +
//      s"AND UPPER(apptypeid) NOT REGEXP '^ZJZY|^GSIOS' " +
//      s"AND UPPER(apptypeid) NOT IN ( " +
//      s"'XWZXTT','XNTTZK','JSSTT','JS_DFTT','USATT','HBJTT','MOPSHIPIN','YZSTT',"+
//      s"'DBHTT','GSGTT','NCJTT','GZGTT','YNYTT','ICANTT','JXGTT','HNYTT', " +
//      s"'HNQTT','XNBLW','XGGTT','GDYTT','TQYB','TQKB','WNWB','GSBROWSER'" +
//      s") ) OR (qid<>'null'   AND qid is NOT null) )"
      )



      strBuild.append(s"and dateline <='${dateLineUp}' " +
        s"and dateline >='${dateLineDown}'")

    val sql =strBuild.toString()
    println(sql)
    val data: Dataset[Row] = spark.sql(sql).coalesce(300)
//    val  dataRDD =data.rdd
//    dataRDD
    val datafilter = data.rdd.filter(
    row =>{
      val qid =row.getString(2)
      if( qid ==null||qid.equals("null")) {
        var apptypeid=row.getString(4)
        if(apptypeid==null){
          true
        }else {
        apptypeid=apptypeid.toUpperCase()
          if(apptypeid.length>=4&&apptypeid.substring(0,4).equals("ZJZY")){
            false
          }else if (apptypeid.length>=5&&apptypeid.substring(0,5).equals("GSIOS")){
            false
          } else if(apptypeid.equals("DFTT")||apptypeid.equals("TTKB")||apptypeid.equals("XWZXTT")||apptypeid.equals("XNTTZK")
            ||apptypeid.equals("JSSTT")||apptypeid.equals("JS_DFTT")||apptypeid.equals("USATT")||apptypeid.equals("HBJTT")
            ||apptypeid.equals("MOPSHIPIN")||apptypeid.equals("YZSTT")||apptypeid.equals("DBHTT")||apptypeid.equals("GSGTT")
            ||apptypeid.equals("NCJTT")||apptypeid.equals("GZGTT")||apptypeid.equals("YNYTT")||apptypeid.equals("ICANTT")
            ||apptypeid.equals("JXGTT")||apptypeid.equals("HNYTT")||apptypeid.equals("HNQTT")||apptypeid.equals("XNBLW")
            ||apptypeid.equals("GDYTT")||apptypeid.equals("TQYB")||apptypeid.equals("TQKB")||apptypeid.equals("WNWB")
            ||apptypeid.equals("GSBROWSER")||apptypeid.equals("XGGTT")){
            false
          }else{
            true
          }
        }
      }else{
        true
      }
    }
    ).filter(row => ETLUtil.filter(row.getString(2)))
      .map(
        cur => {
              val dateline=cur.getLong(0)
              val clientip=cur.getString(1)
              var qid=cur.getString(2)
              if(qid ==null ){
                qid ="null"
              }
              var uid =cur.getString(3)
              val clientime =cur.getString(5)
              if(qid.toLowerCase().equals("huitt")){
                uid =clientime
              }

              Row(dateline,clientip,qid,uid)
        }
      ).repartition(100)
    datafilter
  }

  /**
   *
   * @param spark
   * @param dt
   * @param dateLineDown
   * @param dateLineUp
   * @return
   */
  def getDataForActive(spark:SparkSession  ,dt:String,dateLineDown:Long,dateLineUp:Long):RDD[(Row)]={


    spark.sqlContext.setConf("hive.merge.mapfiles","true")
    spark.sqlContext.setConf("mapred.max.split.size","536870912")
    spark.sqlContext.setConf("mapred.min.split.size.per.node","536870912")
    spark.sqlContext.setConf("mapred.min.split.size.per.rack","536870912")
    spark.sqlContext.setConf("hive.input.format","org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    val strBuild =new StringBuilder("")
    spark.sql(s"use ${ConfigurationManager.getString(Constract.HIVE_DATABASE)}")

    strBuild.append(
      s"select dateline,uid,qid "+
        s"from " +
        s"${ConfigurationManager.getString(Constract.HIVE_H5_ACTIVE_TABLE)} " +
        s"where dt ='${dt}' "
    )
    strBuild.append(s"and dateline <='${dateLineUp}' " +
      s"and dateline >='${dateLineDown}'")

    val sql =strBuild.toString()
    println(sql)
    val data: Dataset[Row] = spark.sql(sql).coalesce(300)

    val datafilter = data.rdd.filter(row => ETLUtil.filter(row.getString(2)))
      .map(
        cur => {
          val dateline=cur.getLong(0)
          val uid=cur.getString(1)
          var qid=cur.getString(2)
          if(qid ==null ){
            qid ="null"
          }

          Row(dateline,null,qid,uid)
        }
      ).repartition(100)
    datafilter
  }

  /**
   * 数据去重< uid!qid ,0>
   * @param dataRDD
   * @return
   */
  def distinctByUIdAndQId(dataRDD:RDD[(Row)]):RDD[((String,String),Long)]={
    val uidRDDqid: RDD[((String,String), Long)] = dataRDD.map(
      row => {

        val uid =row.getString(3)
        val qid =row.getString(2).toLowerCase()
        ((uid,qid),1.toLong)
      }
    )
    val groupbyUidQidRDD: RDD[((String,String), Long)] = uidRDDqid.reduceByKey(_ + _,75)
    groupbyUidQidRDD
  }

  /**
   *
   * @param dataRDD
   * @return
   */
  def distinctByIpAndQId(dataRDD:RDD[(Row)]):RDD[((String,String),Long)]={
    val ipRDDqid: RDD[((String,String), Long)] = dataRDD.map(
      row => {
        val ip =row.getString(1)
        val qid =row.getString(2).toLowerCase()
        ((ip,qid),1.toLong)

      }
    )
    val groupbyIpQidRDD: RDD[((String,String), Long)] = ipRDDqid.reduceByKey(_ + _,75)
    groupbyIpQidRDD
  }

  /**
   * 根据企业ID:qid进行聚合统计
   * @param groupbyUidQidRDD 已经根据uid!qid去重后的RDD
   * @return
   */
  def getAggr2qidRDD(groupbyUidQidRDD:RDD[((String,String),Long)]):RDD[(String,Long)]= {
    //降数据倾斜聚合 <num+qid,1>
//    val key2NumQidRDD: RDD[(String, Long)] = groupbyUidQidRDD.map {
//      case ((uid,qid) ,sum ) => {
//
//        val numQid = s"${random.nextInt(9)}#${QidAndUid.split("!")(1)}"
//        // val uid = QidAndUid.split("_")(1)
//        (numQid, 1.toLong)
//      }
//    }
//    val numQidAggrRDD: RDD[(String, Long)] = key2NumQidRDD.reduceByKey(_ + _)

    //将第一遍统计的数据在聚合
    val key2QidRDD: RDD[(String, Long)] = groupbyUidQidRDD.map {
      case ((_,qid ),_ ) => {

        (qid, 1.toLong)
      }
    }

    val groupBYqidRDD: RDD[(String, Long)] = key2QidRDD.reduceByKey(_ + _)
    groupBYqidRDD
  }

  /**
   * 根据企业ID:qid进行聚合统计
   * @param groupbyUidQidRDD
   * @return
   */
  def getAggr2qid4PvRDD(groupbyUidQidRDD:RDD[((String,String),Long)]):RDD[(String,Long)]={
    //降数据倾斜聚合 <num+qid,1>
    val key2NumQidRDD: RDD[(String, Long)] = groupbyUidQidRDD.map {
      case ((_,qid) , value )=> {
        (qid, value)
      }
    }
   val resultRDD: RDD[(String, Long)] = key2NumQidRDD.reduceByKey(_ + _)
    resultRDD
  }
  /**
   * 获取总UV
   * @param groupbyUidQidRDD  已经根据uid!qid去重后的RDD
   * @return
   */
  def getAggr2SumUV(groupbyUidQidRDD:RDD[((String,String),Long)]) :Long ={
    //当前时间总UV
    val key2UidRDD :RDD[(String ,Long)]=groupbyUidQidRDD.map {
      case ((uid,_),_) => {
        (uid, 1.toLong)
      }
    }
    val groupBYuidRDD: RDD[(String, Long)] = key2UidRDD.reduceByKey(_ + _)
    groupBYuidRDD.count()
  }

  /**
   * 获取总IP
   * @param groupbyIpQidRDD
   * @return
   */
  def getAggr2SumIP(groupbyIpQidRDD:RDD[((String,String),Long)]):Long ={
    //当前时间总UV
    val key2IpRDD :RDD[(String ,Long)]=groupbyIpQidRDD.map {
      case ((ip,_),_) => {
        (ip, 1.toLong)
      }
    }
    val groupByIpRDD: RDD[(String, Long)] = key2IpRDD.reduceByKey(_ + _)
    groupByIpRDD.count()
  }


}
