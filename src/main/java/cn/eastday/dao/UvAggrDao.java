package cn.eastday.dao;

import com.easyday.domain.LogRecord;

import java.util.List;

/**
 * Created by admin on 2018/4/11.
 */
public interface UvAggrDao {
    int  insert(LogRecord logRecord ,String tableName);
    int  delete(int dt,Long logtime,String tableName) ;
    void  truncate (String tableName);
    List<String> getSDKqid();
}
