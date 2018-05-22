package com.easyday.spark

import java.util.{Date, Random}

import cn.eastday.dao.{UvAggr4AppDao, UvAggrDao}
import com.easyday.conf.ConfigurationManager
import com.easyday.constract.Constract
import com.easyday.dao.DAOFactory
import com.easyday.dao.impl.UvAggrDaoImpl
import com.easyday.domain.{APPLogRecord, LogRecord}
import com.easyday.utils.{ETLUtil, DataUtil, DateUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by admin on 2018/4/9.
 */
object UvAndIP2AnaylzeSpark4APP {
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
       .setAppName(Constract.SPARK_APP_NAME_4_APP)
       .set(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES
            ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES))
       .set(Constract.SPARK_SHUFFLE_FILE_BUFFER
            ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_FILE_BUFFER))
       .set(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT
            ,ConfigurationManager.getString(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT))
       .set(Constract.SPARK_SHUFFLE_IO_MAXRETRIES
            ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_IO_MAXRETRIES))
//       .set(Constract.SPARK_DEFAULT_PARALLELISM
//            ,ConfigurationManager.getString(Constract.SPARK_DEFAULT_PARALLELISM))
      // .set("spark.shuffle.manager","hash")
        //.set("spark.shuffle.sort.bypassMergeThreshold","550")
        //.set("spark.shuffle.memoryFraction","0.2")
        //.set("spark.storage.memoryFraction","0.5")


     val sc: SparkContext = SparkContext.getOrCreate(conf)

     val spark: SparkSession = SparkSession.builder()
           .config(conf)
           .enableHiveSupport()
           .getOrCreate()
     // val broadcast :Broadcast[String] =sc.broadcast(dateStr.toString)
    //  logger.debug(s"spark & sparkContext  inited")
     //val random =new Random()
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

       //数据格式：RDD[(Row:[dateline,clientip,appqid,apptypeid,clientime ])]
        var dataRDD:RDD[Row]=getData(spark,dt,dateLineDown,dateLineUp)
        dataRDD =dataRDD.persist(StorageLevel.MEMORY_ONLY)
        dataRDD.first()
        //数据去重<uid!qid>
         var groupbyUidQidRDD: RDD[((String,String,String), Long)] = distinctByUIdAndQId(dataRDD)

        groupbyUidQidRDD=groupbyUidQidRDD.persist(StorageLevel.MEMORY_ONLY)

        //数据去重<IP!qid> job
        var groupbyIpQidRDD: RDD[((String,String,String), Long)] = distinctByIpAndQId(dataRDD)
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
        )


        //公共RDD持久化
        dataTenMinRDD=dataTenMinRDD.persist(StorageLevel.MEMORY_ONLY)


        //聚合distinct
        var groupByUidQidTenMinRDD: RDD[((String,String,String), Long)]= distinctByUIdAndQId(dataTenMinRDD)

        groupByUidQidTenMinRDD=groupByUidQidTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
       // println(s"${DateUtil.date2Str(new Date())}: groupByUidQidTenMinRDD has been peristed ...........")
        var groupByIpQidTenMinRDD: RDD[((String,String,String), Long)]= distinctByIpAndQId(dataTenMinRDD)
        groupByIpQidTenMinRDD=groupByIpQidTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
      //  println(s"${DateUtil.date2Str(new Date())}: groupByIpQidTenMinRDD has been peristed ...........")

        //企业id聚合的UV
        val groupBYqid4UvRDD:RDD[((String,String),Long)]=getAggr2qidRDD(groupbyUidQidRDD)
        val groupBYqid4UvTenMinRDD:RDD[((String,String),Long)]=getAggr2qidRDD(groupByUidQidTenMinRDD)

        //IP
        val groupBYqid4IpRDD:RDD[((String,String),Long)]=getAggr2qidRDD(groupbyIpQidRDD)
        val groupBYqid4IpTenMinRDD:RDD[((String,String),Long)]=getAggr2qidRDD(groupByIpQidTenMinRDD)

        //PV
        var groupBYqid4PvRDD:RDD[((String,String),Long)] =getAggr2qid4PvRDD(groupbyUidQidRDD)
        groupBYqid4PvRDD=groupBYqid4PvRDD.persist(StorageLevel.MEMORY_ONLY)
        var groupBYqid4PvTenMinRDD:RDD[((String,String),Long)] =getAggr2qid4PvRDD(groupByUidQidTenMinRDD)
        groupBYqid4PvTenMinRDD=groupBYqid4PvTenMinRDD.persist(StorageLevel.MEMORY_ONLY)
        //println(s"${DateUtil.date2Str(new Date())} qid4PvRDD has been computed ...........")
        //logger.info(s"${DateUtil.date2Str(new Date())} qid4PvRDD has been peristed ...........")



        //总UV
        val total_UV :RDD[(String,Long)]=getAggr2SumUV(groupbyUidQidRDD)
        val total_UV_10m:RDD[(String,Long)]=getAggr2SumUV(groupByUidQidTenMinRDD)
        val total_IP:RDD[(String,Long)]=getAggr2SumIP(groupbyIpQidRDD)
        val total_IP_10m:RDD[(String,Long)]=getAggr2SumIP(groupByIpQidTenMinRDD)
        val total_PV :RDD[(String,Long)]=getAggr2SumPV(groupBYqid4PvRDD)
        val total_PV_10m:RDD[(String,Long)] =getAggr2SumPV(groupBYqid4PvTenMinRDD)

        val resultRDD :RDD[(APPLogRecord)] =groupBYqid4PvRDD.leftOuterJoin(groupBYqid4UvRDD)
          .leftOuterJoin(groupBYqid4IpRDD).leftOuterJoin(groupBYqid4PvTenMinRDD)
          .leftOuterJoin(groupBYqid4UvTenMinRDD).leftOuterJoin(groupBYqid4IpTenMinRDD)
          .map {
            case ((typeid,qid),(((((pv,uv),ip),incr_pv),incr_uv),incr_ip)) => {

              var qid_new  = ""
              if (qid.length > 50) {
                qid_new = qid.substring(0, 50)
              } else {
                qid_new = qid
              }
              var apptypeId_new = ""
              if (typeid.length > 20) {
                apptypeId_new = typeid.substring(0, 20)
              } else {
                apptypeId_new = typeid
              }
              val uv_new: Long = DataUtil.someOrNone(uv, 0)
              val ip_new: Long = DataUtil.someOrNone(ip, 0)
              val incr_pv_new: Long = DataUtil.someOrNone(incr_pv, 0)
              val incr_uv_new: Long = DataUtil.someOrNone(incr_uv, 0)
              val incr_ip_new: Long = DataUtil.someOrNone(incr_ip, 0)
              APPLogRecord(dt.toInt, dateLineUp,
                apptypeId_new, qid_new, pv, uv_new, ip_new, incr_pv_new, incr_uv_new, incr_ip_new
                ,DateUtil.date2Str(new Date(dateLineUp*1000)))
            }
          }
        val resultTotalRDD :RDD[(APPLogRecord)] =total_PV.leftOuterJoin(total_UV)
          .leftOuterJoin(total_IP).leftOuterJoin(total_PV_10m)
          .leftOuterJoin(total_UV_10m).leftOuterJoin(total_IP_10m)
          .map {
            case (typeid,(((((pv,uv),ip),incr_pv),incr_uv),incr_ip)) => {
              var apptypeId_new = ""
              if (typeid.length > 50) {
                apptypeId_new = typeid.substring(0, 50)
              } else {
                apptypeId_new = typeid
              }
              val uv_new: Long = DataUtil.someOrNone(uv, 0)
              val ip_new: Long = DataUtil.someOrNone(ip, 0)
              val incr_pv_new: Long = DataUtil.someOrNone(incr_pv, 0)
              val incr_uv_new: Long = DataUtil.someOrNone(incr_uv, 0)
              val incr_ip_new: Long = DataUtil.someOrNone(incr_ip, 0)
              APPLogRecord(dt.toInt, dateLineUp,
                apptypeId_new, "total", pv, uv_new, ip_new, incr_pv_new, incr_uv_new, incr_ip_new
                ,DateUtil.date2Str(new Date(dateLineUp*1000)) )
            }
          }
        dataRDD.unpersist()
//        resultRDD.foreachPartition(partition =>{
//         val uvdao :UvAggrDaoImpl =DAOFactory.getUvAggrDao()
//            while(partition.hasNext){
//              val row =partition.next()
//              uvdao.insert(row)
//            }
//        })
        //插入mysql数据库
        val uv4AppDao =DAOFactory.getUvAggr4AppDao()
        val tableName =ConfigurationManager.getString(Constract.APP_TABLE_NAME)
        val tableName2 =ConfigurationManager.getString(Constract.APP_TABLE_NAME2)
        uv4AppDao.delete(dt.toInt,dateLineUp,tableName)
        //
//        resultRDD.union(resultTotalRDD).foreachPartition(
//          iter => {
//            val uv4Appdao1: UvAggr4AppDao = DAOFactory.getUvAggr4AppDao()
//            iter.map {
//              case (row) => {
//                uv4Appdao1.insert(row, tableName)
//              }
//            }
//          })
        resultRDD.union(resultTotalRDD).collect().foreach(
          row => {
            val uv4Appdao1: UvAggr4AppDao = DAOFactory.getUvAggr4AppDao()
            uv4Appdao1.insert(row, tableName)
            }
        )
        val mid4AppDao =DAOFactory.getMiddle4AppDao()
        mid4AppDao.getDateInsert(dt.toInt,dateLineUp,tableName,tableName2)



        val activeRDD =getDataForActive(spark,dt,dateLineDown,dateLineUp)
         activeRDD.persist()

        val groupbyIpQidRDDForActive: RDD[((String,String,String), Long)] = distinctByUIdAndQId(activeRDD)
        groupbyIpQidRDDForActive.persist()

        val dataTenMinRDDForActive =activeRDD.filter(
            row=>{
            val dateline:Long =row.getLong(0)
            if(dateline <= upLine && dateline >downLine  ){
              true
            }else{
              false
            }
          }
        )

        dataTenMinRDDForActive.persist()


        val groupByUidQidTenMinRDDForActive: RDD[((String,String,String), Long)] = distinctByUIdAndQId(dataTenMinRDDForActive)
        groupByUidQidTenMinRDDForActive.persist()
        val groupBYqid4UvRDDForActive:RDD[((String,String),Long)]=getAggr2qidRDD(groupbyIpQidRDDForActive)
        val groupBYqid4UvTenMinRDDForActive:RDD[((String,String),Long)]=getAggr2qidRDD(groupByUidQidTenMinRDDForActive)
        val total_UV_active :RDD[(String,Long)]=getAggr2SumUV(groupbyIpQidRDDForActive)
        val total_UV_10m_active:RDD[(String,Long)]=getAggr2SumUV(groupByUidQidTenMinRDDForActive)
        groupbyIpQidRDDForActive.unpersist()
        groupByUidQidTenMinRDDForActive.unpersist()

        val resultRDDForActive = groupBYqid4UvRDDForActive.leftOuterJoin(groupBYqid4UvTenMinRDDForActive)
          .map{
          case ((apptypeid,qid),(uv,isZero_incr_uv))=>{
            var qid_new  = ""
            if (qid.length > 50) {
              qid_new = qid.substring(0, 50)
            } else {
              qid_new = qid
            }
            var apptypeId_new = ""
            if (apptypeid.length > 20) {
              apptypeId_new = apptypeid.substring(0, 20)
            } else {
              apptypeId_new = apptypeid
            }
            val incr_uv =DataUtil.someOrNone(isZero_incr_uv,0)
            APPLogRecord(dt.toInt, dateLineUp, apptypeId_new, qid_new, 0, uv, 0, 0, incr_uv, 0
              ,DateUtil.date2Str(new Date(dateLineUp*1000)))
          }
        }

        val resultTotalRDDForActive = total_UV_active.leftOuterJoin(total_UV_10m_active)
          .map{
          case ((apptypeid),(uv,isZero_incr_uv))=>{
            var apptypeId_new = ""
            if (apptypeid.length > 20) {
              apptypeId_new = apptypeid.substring(0, 20)
            } else {
              apptypeId_new = apptypeid
            }
            val incr_uv =DataUtil.someOrNone(isZero_incr_uv,0)
            APPLogRecord(dt.toInt, dateLineUp, apptypeId_new, "total", 0, uv, 0, 0, incr_uv, 0
              ,DateUtil.date2Str(new Date(dateLineUp*1000)))
          }
        }
        val active_table =ConfigurationManager.getString(Constract.APP_ACTIVE_TABLE)
        val active_table2 =ConfigurationManager.getString(Constract.APP_ACTIVE_TABLE2)
        val activeJdbcHelperForDelete = DAOFactory.getUvAggr4AppDao()
        activeJdbcHelperForDelete.delete(dt.toInt,dateLineUp,active_table)
        activeRDD.unpersist()
        resultRDDForActive.union(resultTotalRDDForActive).collect().foreach(
           row => {
             val activeJdbcHelper = DAOFactory.getUvAggr4AppDao()
              activeJdbcHelper.insert(row,active_table)
            }
          )
        val mid4AppDaoForActive =DAOFactory.getMiddle4AppDao()
        mid4AppDaoForActive.getDateInsert(dt.toInt,dateLineUp,active_table,active_table2)
     }catch{
       case e :Exception => {
        e.printStackTrace()

         System.exit(-1)
         sc.stop()
         spark.stop()
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
//
    strBuild.append(
      s"select dateline,clientip,appqid,apptypeid,clientime  from " +
      s"${ConfigurationManager.getString(Constract.HIVE_TABLE)} " +
      s"where dt ='${dt}' "
//        +
//        s"and UPPER(apptypeid) like '%TT%' and (clientime)"

      )
      strBuild.append(s"and dateline <='${dateLineUp}' " +
        s"and dateline >='${dateLineDown}'")

    val sql =strBuild.toString()
    println(sql)
    val data: Dataset[Row] = spark.sql(sql).coalesce(300)
    val  dataRDD =data.rdd.filter(
      row =>{
        val clientIme =row.getString(4)

        if(clientIme ==null||clientIme.equals("null")){
          false
        }else{
          true
        }
      }

    ).filter(row=>{
      val apptypeid =row.getString(3)
      if(apptypeid==null || !apptypeid.toUpperCase().contains("TT")){
        false
      }else{
        if(apptypeid.toLowerCase().equals("huitt")){
          false
        }else{
          true
        }
      }
    }).filter(row => ETLUtil.filter(row.getString(2))).repartition(100)
    dataRDD

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
    //
    strBuild.append(
      s"select dateline,uid,qid,softname,softid  from " +
        s"${ConfigurationManager.getString(Constract.HIVE_ACTIVE_TABLE)} " +
        s"where dt ='${dt}' "

    )
    strBuild.append(s"and dateline <='${dateLineUp}' " +
      s"and dateline >='${dateLineDown}'")

    val sql =strBuild.toString()
    println(sql)
    val data: Dataset[Row] = spark.sql(sql).coalesce(300)
    val  dataRDD =data.rdd.map(f=>{
      val dateLine =f.getLong(0)
      val uid =f.getString(1)
      var qid =f.getString(2)
      var softname =f.getString(3)
      var softid =f.getString(4)
      if(softid ==null){
        softid="null"
      }
      if(softname==null ){
        softname="null"
      }
      if(qid ==null ){
        qid ="null"
      }else{
        qid =ETLUtil.trimDate(qid.replace("h5bt","h5"))
      }
      var apptypeid =""
      if(softname.equals("gsbrowser") ||softname.equals("ltbrowser")){
       apptypeid=softname
      }else{
        var length =0
        if(softid.contains("Android")){
          length =7
        }else if(softid.contains("IOS")){
          length=3
        }else{ length=0 }

        apptypeid=softid.substring(0,softid.length-length)
      }

      Row(dateLine,null,qid,apptypeid,uid)
    }).filter(row => ETLUtil.filter(row.getString(2))).repartition(100)
    dataRDD

  }



  /**
   * 数据去重< ime!typeid@qid ,1>
   * @param dataRDD
   * @return
   */
  def distinctByUIdAndQId(dataRDD:RDD[(Row)]):RDD[((String,String,String),Long)]={
    val uidRDDqid: RDD[((String,String,String), Long)] = dataRDD.map(
      row => {
        //原始数据[(dateline,clientip,appqid,apptypeid,clientime)]
        val uid =row.getString(4)
        val qid =ETLUtil.trimDate(row.getString(2)).toLowerCase()
        var typeId =row.getString(3)
        if(typeId==null){
          typeId="null"
        }
//        val key = s"${uid}!${typeId.toUpperCase()}@${qid}"
        //(uid!typeid@qid,1)
        ((uid,typeId.toUpperCase(),qid),1.toLong)
      }
    )
    val groupbyUidQidRDD: RDD[((String,String,String), Long)] = uidRDDqid.reduceByKey(_ + _,75)
    groupbyUidQidRDD
  }

  /**
   *
   * @param dataRDD
   * @return
   */
  def distinctByIpAndQId(dataRDD:RDD[(Row)]):RDD[((String,String,String),Long)]={
    val ipRDDqid: RDD[((String,String,String), Long)] = dataRDD.map(
      row => {
        val ip =row.getString(1)
        val qid =ETLUtil.trimDate(row.getString(2)).toLowerCase()
        var typeId =row.getString(3)
        if(typeId==null){
          typeId="null"
        }
       // val key = s"${ip}!${typeId.toUpperCase()}@${qid}"
        ((ip,typeId.toUpperCase(),qid),1.toLong)
      }
    )
    val groupbyIpQidRDD: RDD[((String,String,String), Long)] = ipRDDqid.reduceByKey(_ + _,75)
    groupbyIpQidRDD
  }

  /**
   * 根据企业ID:qid进行聚合统计
   * @param groupbyUidQidRDD 已经根据uid!qid去重后的RDD

   * @return
   */
  def getAggr2qidRDD(groupbyUidQidRDD:RDD[((String,String,String),Long)]):RDD[((String,String),Long)]= {
    /**
     * 原始数据
     * 降数据倾斜聚合 <num+qid,1>
     */

//    val key2NumQidRDD: RDD[(String, Long)] = groupbyUidQidRDD.map(
//      row => {
//        val QidAndUid = row._1
//        val numQid = s"${random.nextInt(9)}#${QidAndUid.split("!")(1)}"
//        // val uid = QidAndUid.split("_")(1)
//        (numQid, 1.toLong)
//      }
//    )
//    val numQidAggrRDD: RDD[(String, Long)] = key2NumQidRDD.reduceByKey(_ + _)

    //将第一遍统计的数据在聚合
    val key2QidRDD: RDD[((String,String), Long)] = groupbyUidQidRDD.map {
      case((uid,typeid,qid ),_) => {
        //val qid = numQid.split("#")(1)
        // val uid = QidAndUid.split("_")(1)
        ((typeid,qid), 1.toLong)
      }
    }

    val groupBYqidRDD: RDD[((String,String), Long)] = key2QidRDD.reduceByKey(_ + _)
    groupBYqidRDD
  }

  /**
   * 根据企业ID:qid进行聚合统计
   * @param groupbyUidQidRDD
   * @return
   */
  def getAggr2qid4PvRDD(groupbyUidQidRDD:RDD[((String,String,String),Long)]):RDD[((String,String),Long)]={
    //降数据倾斜聚合 <num+qid,1>
    val key2NumQidRDD: RDD[((String,String), Long)] = groupbyUidQidRDD.map {
      case ((_,typeid,qid),value) => {
        ((typeid,qid), value)
      }
    }
   val resultRDD: RDD[((String,String), Long)] = key2NumQidRDD.reduceByKey(_ + _)
    resultRDD
  }
  /**
   * 获取总UV
   * @param groupbyUidQidRDD  已经根据uid!qid去重后的RDD
   * @return
   */
  def getAggr2SumUV(groupbyUidQidRDD:RDD[((String,String,String),Long)]) :RDD[(String,Long)] ={
    //当前时间总UV
    val key2UidRDD :RDD[((String,String) ,Long)]=groupbyUidQidRDD.map {
      case ((uid,typeid,_),_) => {
        ((uid,typeid), 0.toLong)
      }
    }
    val groupBYuidRDD: RDD[((String,String), Long)] = key2UidRDD.reduceByKey(_ + _)
    groupBYuidRDD.map {
      case ((_,typeid),_) => {
        (typeid, 1.toLong)
      }
    }.reduceByKey(_ + _)

  }

  /**
   * 获取总IP
   * @param groupbyIpQidRDD
   * @return
   */
  def getAggr2SumIP(groupbyIpQidRDD:RDD[((String,String,String),Long)]):RDD[(String,Long)] ={
    //当前时间总UV
    val key2IpRDD :RDD[((String,String) ,Long)]=groupbyIpQidRDD.map {
      case ((ip,typeid,_),_) => {
//        val ipAndQid = row._1
//        // val qid = QidAndUid.split("_")(0)
//        val ip = ipAndQid.split("!")(0)
//        val apptypeId = ipAndQid.split("!")(1).split("@")(0)
        ((ip,typeid), 0.toLong)
      }
    }
    val groupByIpRDD: RDD[((String,String), Long)] = key2IpRDD.reduceByKey(_ + _)
    groupByIpRDD.map{
      case ((_,typeid),_)=>{
        (typeid,1.toLong)
      }
    }.reduceByKey(_ + _)
  }

  /**
   * 获取总UV
   * @param groupbyUidQidRDD  已经根据uid!qid去重后的RDD
   * @return
   */
  def getAggr2SumPV(groupbyUidQidRDD:RDD[((String,String),Long)]) :RDD[(String,Long)] ={
    //当前时间总UV
    val key2UidRDD :RDD[(String ,Long)]=groupbyUidQidRDD.map {
      case (( typeid,_), value) => {
        //        val QidAndUid = row._1
        //        val apptypeId = QidAndUid.split("@")(0)
        (typeid, value)
      }
    }
    val groupBYuidRDD: RDD[(String, Long)] = key2UidRDD.reduceByKey(_ + _)
    groupBYuidRDD
  }
}
