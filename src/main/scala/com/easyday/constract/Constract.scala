package com.easyday.constract

/**
 * Created by admin on 2018/4/3.
 */
object Constract {

  val SPARK_IS_LOCAL = "spark.islocal"
  val SPARK_APP_NAME = "UvAndIP2AnaylzeSpark"
  val SPARK_APP_NAME_4_APP = "UvAndIP2AnaylzeSpark4APP"
  val SPARK_APP_NAME_NEWS = "UvAndIP2AnaylzeSpark4APPNews"
  val SPARK_APP_NAME_4_STREAMING = "PvAndUv4H5Streaming"
  val SPARK_APP_NAME_RECOMMEND = "Recommend4Data"
  val HIVE_DATABASE ="hive.database"
  val HIVE_TABLE="hive.table"
  val HIVE_H5_TABLE="hive.h5.table"
  val HIVE_ACTIVE_TABLE ="hive.active.table"
  val HIVE_H5_ACTIVE_TABLE="hive.h5.active.table"
  val TIME_INTERVAL ="time.interval"


  /**
   * 项目配置中的常量
   */
  val JDBC_DRIVER= "jdbc.driver";
  val JDBC_DATASOURCE_SIZE="jdbc.datasource.size";
  val JDBC_URL ="jdbc.url";
  val JDBC_USER="jdbc.user";
  val JDBC_PASSWD="jdbc.passwd";
  val H5_TABLE_NAME="h5.table.name"
  val H5_TABLE_NAME2="h5.table.name2"
  val APP_TABLE_NAME="app.table.name"
  val APP_TABLE_NAME2="app.table.name2"
  val APP_ACTIVE_TABLE="app.active.table"
  val APP_ACTIVE_TABLE2="app.active.table2"
  val APP_NEWS_TABLE="app.news.table"
  val APP_NEWS_TABLE2="app.news.table2"
  //hadoop配置参数
  val DFS_CLIENT_SOCKET_TIMEOUT="dfs.client.socket-timeout"
  //spark配置参数
  val SPARK_SHUFFLE_CONSOLIDATEFILES ="spark.shuffle.consolidateFiles"
  val SPARK_SHUFFLE_FILE_BUFFER="spark.shuffle.file.buffer"
  val SPARK_REDUCER_MAXSIZEINFLIGHT="spark.reducer.maxSizeInFlight"
  val SPARK_SHUFFLE_IO_MAXRETRIES="spark.shuffle.io.maxRetries"
  val SPARK_DEFAULT_PARALLELISM="spark.default.parallelism"
  //kafka信息
  val KAFKA_TOPICS="kafka.topics"
  val OFFSET_TABLE="offset.table"

}
