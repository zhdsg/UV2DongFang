package cn.eastday.dao.Impl;


import cn.eastday.dao.UvAggrDao;
import cn.eastday.jdbc.JDBCHelper;
import com.easyday.conf.ConfigurationManager;
import com.easyday.constract.Constract;
import com.easyday.dao.DAOFactory;
import com.easyday.domain.LogRecord;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2018/4/11.
 */
public class UvAggrDaoImpl implements UvAggrDao {
  JDBCHelper jdbcHelper =JDBCHelper.getInstance();

  @Override
  public int insert(LogRecord logRecord,String tableName) {
    String sql ="insert into "+tableName
            +" (dt,currenttime,qid,pv,uv,ip,incr_pv,incr_uv,incr_ip,activecnt,incr_activecnt) "
            +"values (?,?,?,?,?,?,?,?,?,?,?)";
    Object[] params =new Object[]{
            logRecord.dt(), logRecord.currentTime(), logRecord.qId(), logRecord.PV(),logRecord.UV(),
            logRecord.IP(),logRecord.incrPV(),logRecord.incrUV(),logRecord.incrIP(),logRecord.activecnt(),
            logRecord.incr_activecnt()
    };
    return jdbcHelper.executeUpdate(sql ,params);
  }

  @Override
  public int delete(int dt, Long logtime,String tableName) {
    String sql ="delete from "+tableName+" where dt =? and currenttime=?";
    Object[] param =new Object[]{dt,logtime};
    return  jdbcHelper.executeUpdate(sql,param);
  }

  @Override
  public void truncate(String tableName) {
    String sql ="truncate table "+ tableName;
    jdbcHelper.executeUpdate(sql, new Object[]{});
  }

  @Override
  public ArrayList<String> getSDKqid() {
    String sql =" select h5qid from dataconfig.config_sdk_qid";
    final ArrayList<String> list  =new ArrayList<>();
    jdbcHelper.executeQuery(sql, new Object[]{}, new JDBCHelper.QueryCallback() {
      @Override
      public void process(ResultSet rs) throws Exception {
        while(rs.next()){
          list.add(rs.getString(1));
        }
      }
    });
    return list;
  }

  public static void main(String[] args ){
    //UvAggrDao uvAggrDao = DAOFactory.getUvAggrDao();
    //uvAggrDao.insert(new LogRecord(1,2,"1",4,5,5,6,7,7,8,8),ConfigurationManager.getString(Constract.H5_TABLE_NAME()));
    //uvAggrDao.truncate(ConfigurationManager.getString(Constract.H5_TABLE_NAME()));
  }
}

