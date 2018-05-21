package cn.eastday.dao.Impl;


import cn.eastday.dao.UvAggrDao;
import cn.eastday.jdbc.JDBCHelper;
import com.easyday.conf.ConfigurationManager;
import com.easyday.constract.Constract;
import com.easyday.dao.DAOFactory;
import com.easyday.domain.LogRecord;

/**
 * Created by admin on 2018/4/11.
 */
public class UvAggrDaoImpl implements UvAggrDao {
  JDBCHelper jdbcHelper =JDBCHelper.getInstance();

  @Override
  public int insert(LogRecord logRecord,String tableName) {
    String sql ="insert into "+tableName
            +" (dt,logtime,qid,pv,uv,ip,incr_pv,incr_uv,incr_ip) "
            +"values (?,?,?,?,?,?,?,?,?)";
    Object[] params =new Object[]{
            logRecord.dt(), logRecord.logTime(), logRecord.qId(), logRecord.PV(),logRecord.UV(),
            logRecord.IP(),logRecord.incrPV(),logRecord.incrUV(),logRecord.incrIP()
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

