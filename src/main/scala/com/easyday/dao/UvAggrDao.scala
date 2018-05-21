package com.easyday.dao

import com.easyday.domain.LogRecord

/**
 * Created by admin on 2018/4/11.
 * 4/23.迁移值java目录下
 */
trait UvAggrDao {
    def insert(logRecord :LogRecord):Int
    def delete(dt:Int,logtime:Long) :Int
    def truncate (): Unit

}
