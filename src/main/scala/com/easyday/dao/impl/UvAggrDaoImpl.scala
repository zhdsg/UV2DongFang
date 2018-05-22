package com.easyday.dao.impl

import com.easyday.conf.ConfigurationManager
import com.easyday.constract.Constract
import com.easyday.dao.UvAggrDao
import com.easyday.domain.LogRecord
import com.easyday.utils.JDBCHelper

/**
 * Created by admin on 2018/4/11.
 *
 * 4/23.迁移值java目录下
 */
class UvAggrDaoImpl extends UvAggrDao{
  val jdbcHelper :JDBCHelper =JDBCHelper.apply()

  override def insert(logRecord: LogRecord): Int = {
      val sql =s"insert into ${ConfigurationManager.getString(Constract.H5_TABLE_NAME)} (dt,logtime,qid,pv,uv,ip,incr_pv,incr_uv,incr_ip) " +
        s"values (${logRecord.dt},${logRecord.currentTime},'${logRecord.qId.replace("'","")}',${logRecord.PV},${logRecord.UV}" +
        s",${logRecord.IP},${logRecord.incrPV},${logRecord.incrUV},${logRecord.incrIP}); "

    jdbcHelper.executeUpdate(sql )

  }

  override def delete(dt: Int, logtime: Long): Int = {
    val sql =s"delete from ${ConfigurationManager.getString(Constract.H5_TABLE_NAME)} where dt =${dt} and logtime=${logtime}"
    jdbcHelper.executeUpdate(sql)
  }

  override def truncate(): Unit = {
    val sql =s"truncate table ${ConfigurationManager.getString(Constract.H5_TABLE_NAME)}"
    jdbcHelper.executeUpdate(sql)
  }
}

