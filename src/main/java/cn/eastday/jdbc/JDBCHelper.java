package cn.eastday.jdbc;

/**
 * Created by Administrator on 2018/1/26/026.
 */



import com.easyday.conf.ConfigurationManager;
import com.easyday.constract.Constract;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Jdbc辅助组件
 */

 //项目尽量做成可配置的
public class JDBCHelper {

    static {
        try{
            String driver = ConfigurationManager.getString(Constract.JDBC_DRIVER());
            Class.forName(driver);
        }catch(Exception e){
            e.printStackTrace();
        }

    }
    //实现JDBC的单例话
    private volatile static JDBCHelper instance =null;
    private LinkedList<Connection> datasource =new LinkedList<Connection>();
    private JDBCHelper(){
        /**
         * 创建连接池
         */
        int datasourceSize =ConfigurationManager.getInteger(Constract.JDBC_DATASOURCE_SIZE());
        for(int i =0;i < datasourceSize ; i++){
            try {
                String url =ConfigurationManager.getString(Constract.JDBC_URL());
                String user =ConfigurationManager.getString(Constract.JDBC_USER());
                String passwd =ConfigurationManager.getString(Constract.JDBC_PASSWD());
                Connection conn = DriverManager.getConnection(
                        url,
                        user,
                        passwd
                );
                datasource.add(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }

    /**
     * 获取单例
     * @return
     */
    public static JDBCHelper getInstance(){

        if(instance == null){
            synchronized(JDBCHelper.class){
                if(instance == null){
                    instance =new JDBCHelper();
                }
            }
        }
        return instance;
    }

    /**
     * 获取数据库连接
     * @return
     */
    public synchronized Connection getConnection(){

        while(datasource.size() ==0){
            try{
                Thread.sleep(10);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 开发增删改查的方法
     * 1.执行增删改sql语句的方法
     * 2.执行查询sql语句的方法
     * 3.批量执行sql语句的方法
     */
    public int executeUpdate(String sql ,Object[] params){
        Connection conn =null ;
        PreparedStatement pstmt =null ;
        int rtn =0;
        try {
            conn =getConnection();
            pstmt =conn.prepareStatement(sql);
            for(int i = 0;i < params.length;i++){
                pstmt.setObject(i + 1,params[i]);
            }
            rtn = pstmt.executeUpdate();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(conn !=null){
                datasource.push(conn);
            }

        }
        return rtn ;

    }

    /**
     * 查询sql语句的执行方法
     * @param sql
     * @param params
     * @param callback
     */


    public  void executeQuery(String sql ,Object[] params,QueryCallback callback){
        Connection conn =null;
        PreparedStatement pstmt =null;
        ResultSet rs =null;
        try{
            conn =getConnection();
            pstmt =conn.prepareStatement(sql);
            for(int i = 0; i <params.length ; i++){
                pstmt.setObject(i + 1,params[i]);
            }
            rs =pstmt.executeQuery();
            callback.process(rs);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(conn != null){
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量执行sql语句的方法
     * @param sql
     * @param paramsList
     * @return
     */
    public int[] executeBatch(String sql ,List<Object[]> paramsList){
        int[] rtn =null;
        Connection conn =null;
        PreparedStatement pstmt =null;
        //第一步： 使用Connection对象，
        try{
            conn=getConnection();
            //conn.setAutoCommit(false);
            pstmt=conn.prepareStatement(sql);
            for(Object[] objs :paramsList){
                for(int i =0; i < objs.length; i++){
                    pstmt.setObject(i+1,objs[i]);
                }
                //加入批量的参数
                pstmt.addBatch();
            }
            //使用executeBatch(),批量提交
           rtn= pstmt.executeBatch();
            //最后一步，使用Connection对象提交

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(conn != null){
                datasource.push(conn);
            }
        }
        return rtn;
    }
    /**
     * 内部类：查询回调接口
     */
    public static interface QueryCallback{
        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs)throws Exception;

    }



}
