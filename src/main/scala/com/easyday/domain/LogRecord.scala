package com.easyday.domain

import java.util.Date

/**
 * Created by admin on 2018/4/10.
 */
case class LogRecord(dt:Int,currentTime:Long,qId:String,PV:Long
                      ,UV:Long,IP:Long,incrPV:Long,incrUV:Long,incrIP:Long,activecnt:Long,incr_activecnt:Long)

case class APPLogRecord(dt:Int,logTime:Long,apptypeId:String ,qId:String,PV:Long
                        ,UV:Long,IP:Long,incrPV:Long,incrUV:Long,incrIP:Long,currenttime:String)

//case class H5DataRecord(uid:String,ip :String, qid :String ,dateLine: Long)
//case class AppDataRecord(uid:String, ip :String,apptypeid:String,qid :String ,dateLine: Long)
//
