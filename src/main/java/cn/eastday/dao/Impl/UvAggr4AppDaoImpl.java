package cn.eastday.dao.Impl;


import cn.eastday.dao.UvAggr4AppDao;
import cn.eastday.dao.UvAggrDao;
import cn.eastday.jdbc.JDBCHelper;
import com.easyday.conf.ConfigurationManager;
import com.easyday.constract.Constract;
import com.easyday.dao.DAOFactory;
import com.easyday.domain.APPLogRecord;
import com.easyday.domain.LogRecord;

/**
 * Created by admin on 2018/4/11.
 */
public class UvAggr4AppDaoImpl implements UvAggr4AppDao {
  JDBCHelper jdbcHelper =JDBCHelper.getInstance();

  @Override
  public int insert(APPLogRecord appLogRecord,String tableName) {
    String sql ="insert into "+tableName
            +" (dt,logtime,apptypeid,qid,pv,uv,ip,incr_pv,incr_uv,incr_ip) "
            +"values (?,?,?,?,?,?,?,?,?,?)";
    Object[] params =new Object[]{
            appLogRecord.dt(), appLogRecord.logTime(), appLogRecord.apptypeId(),appLogRecord.qId(), appLogRecord.PV(),appLogRecord.UV(),
            appLogRecord.IP(),appLogRecord.incrPV(),appLogRecord.incrUV(),appLogRecord.incrIP()
    };
    return jdbcHelper.executeUpdate(sql ,params);
  }

  @Override
  public int delete(int dt, Long logtime,String tableName) {
    String sql ="delete from "+tableName+" where dt =? and logtime=?";
    Object[] param =new Object[]{dt,logtime};
    return  jdbcHelper.executeUpdate(sql,param);
  }

  @Override
  public void truncate(String tableName) {
    String sql ="truncate table "+ tableName;
    jdbcHelper.executeUpdate(sql,new Object[]{});
  }
  public static void main(String[] args ){
    UvAggrDao uvAggrDao = DAOFactory.getUvAggrDao();
    uvAggrDao.insert(new LogRecord(1,2,"1",4,5,5,6,7,7),ConfigurationManager.getString(Constract.H5_TABLE_NAME()));
  }
}

