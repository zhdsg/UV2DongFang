package com.easyday.constract

/**
 * Created by admin on 2018/4/3.
 */
object Constract {

  val SPARK_IS_LOCAL = "spark.islocal"
  val SPARK_APP_NAME = "UvAndIP2AnaylzeSpark"
  val HIVE_DATABASE ="hive.database"
  val HIVE_TABLE="hive.table"
  val TIME_INTERVAL ="time.interval"


  /**
   * 项目配置中的常量
   */
  val JDBC_DRIVER= "jdbc.driver";
  val JDBC_DATASOURCE_SIZE="jdbc.datasource.size";
  val JDBC_URL ="jdbc.url";
  val JDBC_USER="jdbc.user";
  val JDBC_PASSWD="jdbc.passwd";
  val TABLE_NAME="table.name"
  val TABLE_NAME2="table.name2"
  //hadoop配置参数
  val DFS_CLIENT_SOCKET_TIMEOUT="dfs.client.socket-timeout"
  //spark配置参数
  val SPARK_SHUFFLE_CONSOLIDATEFILES ="spark.shuffle.consolidateFiles"
  val SPARK_SHUFFLE_FILE_BUFFER="spark.shuffle.file.buffer"
  val SPARK_REDUCER_MAXSIZEINFLIGHT="spark.reducer.maxSizeInFlight"
  val SPARK_SHUFFLE_IO_MAXRETRIES="spark.shuffle.io.maxRetries"
  val SPARK_DEFAULT_PARALLELISM="spark.default.parallelism"


}
