package com.easyday.dao

import cn.eastday.dao.Impl.{Middle4AppDaoImpl, UvAggr4AppDaoImpl, UvAggrDaoImpl, MiddleDaoImpl}


/**
 * Created by admin on 2018/4/11.
 */
object  DAOFactory {

  def getUvAggrDao():UvAggrDaoImpl={
    new UvAggrDaoImpl
  }
  def getMiddleDao():MiddleDaoImpl={
         new MiddleDaoImpl
    }
  def getUvAggr4AppDao():UvAggr4AppDaoImpl={
    new UvAggr4AppDaoImpl
  }
  def getMiddle4AppDao():Middle4AppDaoImpl={
    new Middle4AppDaoImpl
  }
}
