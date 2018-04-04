package com.easyday.utils

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

/**
 * Created by admin on 2018/4/3.
 */
object DateUtil {

  val DATE_FORMAT =new SimpleDateFormat("yyyyMMdd")

  def getYesterdayDate():String={
    val  cal :Calendar =Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DAY_OF_YEAR,-1)

    DATE_FORMAT.format(cal.getTime)
  }
  def getTodayDate() :String ={
    DATE_FORMAT.format(new Date())
  }


}
