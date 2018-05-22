package cn.eastday.dao.Impl;

import cn.eastday.dao.MiddleDao;
import cn.eastday.jdbc.JDBCHelper;
import com.easyday.conf.ConfigurationManager;
import com.easyday.constract.Constract;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by admin on 2018/4/16.
 */
public class MiddleDaoImpl implements MiddleDao {
    private JDBCHelper jdbcHelper =JDBCHelper.getInstance();
    //private Logger logger = Logger.getLogger(MiddleDaoImpl.class);
    @Override
    public int[] getDateInsert(int dt ,Long logtime,String tableName,String tableName2) {
        final List<Object[]> lists = new ArrayList<Object[]>();
        String sql ="select dt,currenttime,qid,max(pv),max(uv),max(ip),max(incr_pv),max(incr_uv),max(incr_ip),max(activecnt),max(incr_activecnt) " +
                "from " + tableName+" where dt=? and currenttime =? group by dt ,currenttime,qid ";
        Object[] params =new Object[]{dt,logtime};
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {

                    Object[] param = new Object[]{
                            rs.getInt(1), rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getLong(5),
                            rs.getLong(6), rs.getLong(7), rs.getLong(8), rs.getLong(9),rs.getLong(10),rs.getLong(11)
                    };
                    lists.add(param);
                }
            }
        });
        //logger.info("query success ....");
        String delSQL= "delete from "+tableName2
                + " where dt =? and currenttime =?";
        int delResult= jdbcHelper.executeUpdate(delSQL,params);
       // logger.info("delete success ....");
        String sql2 ="insert into "+tableName2
                +"(dt,currenttime,qid,pv,uv,ip,incr_pv,incr_uv,incr_ip,activecnt,incr_activecnt) values(?,?,?,?,?,?,?,?,?,?,?)";
        int[] result =jdbcHelper.executeBatch(sql2,lists);
        return result;
    }
}
