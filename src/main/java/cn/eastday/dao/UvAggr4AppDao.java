package cn.eastday.dao;

import com.easyday.domain.APPLogRecord;
import com.easyday.domain.LogRecord;

/**
 * Created by admin on 2018/4/11.
 */
public interface UvAggr4AppDao {
    int  insert(APPLogRecord logRecord, String tableName);
    int  delete(int dt, Long logtime, String tableName) ;
    void  truncate(String tableName);

}
