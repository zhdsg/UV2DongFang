package com.easyday.utils

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

/**
 * Created by admin on 2018/4/3.
 */
object DateUtil {

  val DT_FORMAT =new SimpleDateFormat("yyyyMMdd")
  val TIME_FORMAT =new SimpleDateFormat("yyyy-MM-dd 00:00:00")
  val DATE_FORMAT =new SimpleDateFormat("yyyy-MM-dd 00:00:00")
  def getYesterdayDate():String={
    val  cal :Calendar =Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DAY_OF_YEAR,-1)

    DT_FORMAT.format(cal.getTime)
  }
  def getTodayDate() :String ={
    DT_FORMAT.format(new Date())
  }
  def getZeroTime(date :Date):Date={
    TIME_FORMAT.parse(DATE_FORMAT.format(date))
  }

}
