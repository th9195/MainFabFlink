package com.hzw.fdc.util

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool



/**
 * @author gdj
 * @create 2020-06-06-19:23
 *
 */
object ProjectConfig {
  /**
   * 配置文件 配置名 全小写  用 . 分割
   */

  //RocksDBStateBackend 地址
  var ROCKSDB_HDFS_PATH = "hdfs://nameservice1/flink-checkpoints"
  var FILESYSTEM_HDFS_PATH= "hdfs://nameservice1/flink-checkpoints-filesystem"

  //#Hbase配置  表名命名 需要带_table 后缀
  var HBASE_ZOOKEEPER_QUORUM: String = "fdc01,fdc02,fdc03,fdc04,fdc05"
  var HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT: String = "2181"


  //#KAFKA配置
  var KAFKA_QUORUM: String = "fdc01:9092,fdc02:9092,fdc03:9092"

  var KAFKA_PROPERTIES_RETRIES = "2147483647"
  var KAFKA_PROPERTIES_BUFFER_MEMORY = "67108865"
  var KAFKA_PROPERTIES_BATCH_SIZE = "131073"
  var KAFKA_PROPERTIES_LINGER_MS = "101"
  var KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "2"
  var KAFKA_PROPERTIES_RETRY_BACKOFF_MS = "101"
  var KAFKA_PROPERTIES_MAX_REQUEST_SIZE = "100000001"
  var KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS = "300001"
  var KAFKA_PROPERTIES_COMPRESSION_TYPE = "none"
  var KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_ENABLE = false
  var KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = "30000"

  //配置文件地址
  var PROPERTIES_FILE_PATH: String = _


  // 根据时间戳消费某个kafka topic 的历史数据 用于生成环境排查问题
  var CONSUMER_KAFKA_HISTORY_DATA_TOPIC = "mainfab_data_topic"
  var CONSUMER_KAFKA_HISTORY_DATA_GROUPID = "consumer-group-mainfab-kafka-history-data-job-toby"
  var CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP = System.currentTimeMillis()
  var CONSUMER_KAFKA_HISTORY_DATA_FILTERINFO = ""
  var CONSUMER_KAFKA_HISTORY_DATA_FILE_PATH = "/kafkaHistoryData/"
  var SET_PARALLELISM_OFFLINE_CONSUMER_KAFKA_HISTORY_DATA_JOB = 1
  var CHECKPOINT_ENABLE_OFFLINE_CONSUMER_KAFKA_HISTORY_DATA_JOB = false

  //数据支持长度单位小时
  var RUN_MAX_LENGTH:Int= 26


  //******************************************************* VMC START ***************************************************
  var SET_PARALLELISM_VMC_ALL_JOB = 1
  var CHECKPOINT_ENABLE_VMC_ALL_JOB = true
  var CHECKPOINT_STATE_BACKEND_VMC_ALL_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_VMC_ALL_JOB = 60000
  var CHECKPOINT_TIME_OUT_VMC_ALL_JOB = 600000

  var SET_PARALLELISM_VMC_ETL_JOB = 1
  var CHECKPOINT_ENABLE_VMC_ETL_JOB = true
  var CHECKPOINT_STATE_BACKEND_VMC_ETL_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_VMC_ETL_JOB = 60000
  var CHECKPOINT_TIME_OUT_VMC_ETL_JOB = 600000

  var SET_PARALLELISM_VMC_WINDOW_JOB = 1
  var CHECKPOINT_ENABLE_VMC_WINDOW_JOB = true
  var CHECKPOINT_STATE_BACKEND_VMC_WINDOW_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_VMC_WINDOW_JOB = 120000
  var CHECKPOINT_TIME_OUT_VMC_WINDOW_JOB = 600000

  var SET_PARALLELISM_VMC_INDICATOR_JOB = 1
  var CHECKPOINT_ENABLE_VMC_INDICATOR_JOB = true
  var CHECKPOINT_STATE_BACKEND_VMC_INDICATOR_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_VMC_INDICATOR_JOB = 60000
  var CHECKPOINT_TIME_OUT_VMC_INDICATOR_JOB = 600000


  var KAFKA_CONSUMER_GROUP_VMC_ALL_JOB = "consumer-group-vmc-all-job"
  var KAFKA_CONSUMER_GROUP_VMC_ETL_JOB = "consumer-group-vmc-etl-job"
  var KAFKA_CONSUMER_GROUP_VMC_WINDOW_JOB = "consumer-group-vmc-window-job"
  var KAFKA_CONSUMER_GROUP_VMC_INDICATOR_JOB = "consumer-group-vmc-indicator-job"
  var KAFKA_CONSUMER_GROUP_VMC_WRITE_INDICATOR_JOB = "consumer-group-vmc-write-indicator-job"

  var SET_PARALLELISM_VMC_WRITE_INDICATOR_JOB = 1
  var CHECKPOINT_ENABLE_VMC_WRITE_INDICATOR_JOB = true
  var CHECKPOINT_STATE_BACKEND_VMC_WRITE_INDICATOR_JOB = "rocksdb"
  var CHECKPOINT_INTERVAL_VMC_WRITE_INDICATOR_JOB = 60000
  var CHECKPOINT_TIME_OUT_VMC_WRITE_INDICATOR_JOB = 600000

  var KAFKA_VMC_DATA_TOPIC = "mainfab_data_topic"
  var KAFKA_VMC_ETL_TOPIC = "vmc_etl_topic"
  var KAFKA_VMC_MATCH_CONTROLPLAN_TOPIC = "vmc_match_controlplan_topic"
  var KAFKA_VMC_ADD_STEP_TOPIC = "vmc_add_step_topic"
  var KAFKA_VMC_INDICATOR_TOPIC = "vmc_indicator_topic"


  // oracle
  var VMC_ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver"
  var VMC_ORACLE_URL = "jdbc:oracle:thin:@10.1.10.208:1522:ORCLCDB"
  var VMC_ORACLE_USER: String = "mainfabdev"
  var VMC_ORACLE_PASSWORD: String = "mainfabdev"
  var VMC_ORACLE_POOL_MIN_SIZE = 2
  var VMC_ORACLE_POOL_MAX_SIZE = 5

  var VMC_ETL_DEBUG_EANBLE = true
  var VMC_MATCH_CONTROLPLAN_DEBUG_EANBLE = true
  var VMC_ADD_STEP_DEBUG_EANBLE = true
  var VMC_INDICATOR_DEBUG_EANBLE = false


  var HBASE_VMC_INDICATOR_DATA_TABLE = "vmc_indicator_data_table"
  var INDICATOR_BATCH_WRITE_HBASE_NUM = 500

  //******************************************************* VMC END ***************************************************


  /**
   * 初始化
   */
  def initConfig(): ParameterTool = {
    var configname: ParameterTool = null
    try {
      configname = ParameterTool.fromPropertiesFile(PROPERTIES_FILE_PATH)
      Config(configname)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    configname
  }

  /**
   * 获取变量
   */
  def getConfig(configname: ParameterTool): ParameterTool = {
    Config(configname)
    configname
  }

  def Config(configname: ParameterTool): ParameterTool = {

    ROCKSDB_HDFS_PATH = configname.get("rocksdb.hdfs.path")
    if(configname.has("filesystem.hdfs.path")){
      FILESYSTEM_HDFS_PATH= configname.get("filesystem.hdfs.path", FILESYSTEM_HDFS_PATH)
    }

    //Hbase
    HBASE_ZOOKEEPER_QUORUM = configname.get("hbase.zookeeper.quorum")
    HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = configname.get("hbase.zookeeper.property.clientport")

    //kafka地址
    KAFKA_QUORUM = configname.get("kafka.zookeeper.connect")

    // 根据时间戳消费某个kafka topic 的历史数据 用于生成环境排查问题
    CONSUMER_KAFKA_HISTORY_DATA_TOPIC = configname.get("consumer.kafka.history.data.topic",CONSUMER_KAFKA_HISTORY_DATA_TOPIC).trim
    CONSUMER_KAFKA_HISTORY_DATA_GROUPID = configname.get("consumer.kafka.history.data.groupid",CONSUMER_KAFKA_HISTORY_DATA_GROUPID).trim
    CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP = configname.get("consumer.kafka.history.data.timestamp",CONSUMER_KAFKA_HISTORY_DATA_TIMESTAMP.toString).trim.toLong
    CONSUMER_KAFKA_HISTORY_DATA_FILTERINFO = configname.get("consumer.kafka.history.data.filterinfo",CONSUMER_KAFKA_HISTORY_DATA_FILTERINFO).trim

    // KAFKA 配置
    KAFKA_PROPERTIES_RETRIES = configname.get("kafka.properties.retries",KAFKA_PROPERTIES_RETRIES).trim
    KAFKA_PROPERTIES_BUFFER_MEMORY = configname.get("kafka.properties.buffer.memory",KAFKA_PROPERTIES_BUFFER_MEMORY).trim
    KAFKA_PROPERTIES_BATCH_SIZE = configname.get("kafka.properties.batch.size",KAFKA_PROPERTIES_BATCH_SIZE).trim
    KAFKA_PROPERTIES_LINGER_MS = configname.get("kafka.properties.linger.ms",KAFKA_PROPERTIES_LINGER_MS).trim
    KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = configname.get("kafka.properties.max.in.flight.requests.per.connection",KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).trim
    KAFKA_PROPERTIES_RETRY_BACKOFF_MS = configname.get("kafka.properties.retry.backoff.ms",KAFKA_PROPERTIES_RETRY_BACKOFF_MS).trim
    KAFKA_PROPERTIES_MAX_REQUEST_SIZE = configname.get("kafka.properties.max.request.size",KAFKA_PROPERTIES_MAX_REQUEST_SIZE).trim
    KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS = configname.get("kafka.properties.transaction.timeout.ms",KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS).trim
    KAFKA_PROPERTIES_COMPRESSION_TYPE = configname.get("kafka.properties.compression.type",KAFKA_PROPERTIES_COMPRESSION_TYPE).trim
    KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_ENABLE = configname.get("kafka.properties.key.partition.discovery.enable",KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_ENABLE.toString).trim.toBoolean
    KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = configname.get("kafka.properties.key.partition.discovery.interval.millis",KAFKA_PROPERTIES_KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS).trim

    //******************************************************* VMC START ***************************************************

    SET_PARALLELISM_VMC_ALL_JOB = configname.get("set.parallelism.vmc.all.job",SET_PARALLELISM_VMC_ALL_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_VMC_ALL_JOB = configname.get("checkpoint.enable.vmc.all.job",CHECKPOINT_ENABLE_VMC_ALL_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_VMC_ALL_JOB = configname.get("checkpoint.state.backend.vmc.all.job",CHECKPOINT_STATE_BACKEND_VMC_ALL_JOB).trim
    CHECKPOINT_INTERVAL_VMC_ALL_JOB = configname.get("checkpoint.interval.vmc.all.job",CHECKPOINT_INTERVAL_VMC_ALL_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_VMC_ALL_JOB = configname.get("checkpoint.time.out.vmc.all.job",CHECKPOINT_TIME_OUT_VMC_ALL_JOB.toString).trim.toInt


    SET_PARALLELISM_VMC_ETL_JOB = configname.get("set.parallelism.vmc.etl.job",SET_PARALLELISM_VMC_ETL_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_VMC_ETL_JOB = configname.get("checkpoint.enable.vmc.etl.job",CHECKPOINT_ENABLE_VMC_ETL_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_VMC_ETL_JOB = configname.get("checkpoint.state.backend.vmc.etl.job",CHECKPOINT_STATE_BACKEND_VMC_ETL_JOB).trim
    CHECKPOINT_INTERVAL_VMC_ETL_JOB = configname.get("checkpoint.interval.vmc.etl.job",CHECKPOINT_INTERVAL_VMC_ETL_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_VMC_ETL_JOB = configname.get("checkpoint.time.out.vmc.etl.job",CHECKPOINT_TIME_OUT_VMC_ETL_JOB.toString).trim.toInt

    SET_PARALLELISM_VMC_WINDOW_JOB = configname.get("set.parallelism.vmc.window.job",SET_PARALLELISM_VMC_WINDOW_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_VMC_WINDOW_JOB = configname.get("checkpoint.enable.vmc.window.job",CHECKPOINT_ENABLE_VMC_WINDOW_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_VMC_WINDOW_JOB = configname.get("checkpoint.state.backend.vmc.window.job",CHECKPOINT_STATE_BACKEND_VMC_WINDOW_JOB).trim
    CHECKPOINT_INTERVAL_VMC_WINDOW_JOB = configname.get("checkpoint.interval.vmc.window.job",CHECKPOINT_INTERVAL_VMC_WINDOW_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_VMC_WINDOW_JOB = configname.get("checkpoint.time.out.vmc.window.job",CHECKPOINT_TIME_OUT_VMC_WINDOW_JOB.toString).trim.toInt


    SET_PARALLELISM_VMC_INDICATOR_JOB = configname.get("set.parallelism.vmc.indicator.job",SET_PARALLELISM_VMC_INDICATOR_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_VMC_INDICATOR_JOB = configname.get("checkpoint.enable.vmc.indicator.job",CHECKPOINT_ENABLE_VMC_INDICATOR_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_VMC_INDICATOR_JOB = configname.get("checkpoint.state.backend.vmc.indicator.job",CHECKPOINT_STATE_BACKEND_VMC_INDICATOR_JOB).trim
    CHECKPOINT_INTERVAL_VMC_INDICATOR_JOB = configname.get("checkpoint.interval.vmc.indicator.job",CHECKPOINT_INTERVAL_VMC_INDICATOR_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_VMC_INDICATOR_JOB = configname.get("checkpoint.time.out.vmc.indicator.job",CHECKPOINT_TIME_OUT_VMC_INDICATOR_JOB.toString).trim.toInt

    KAFKA_CONSUMER_GROUP_VMC_ALL_JOB = configname.get("consumer.group.vmc.all.job",KAFKA_CONSUMER_GROUP_VMC_ALL_JOB.toString).trim
    KAFKA_CONSUMER_GROUP_VMC_ETL_JOB = configname.get("consumer.group.vmc.etl.job",KAFKA_CONSUMER_GROUP_VMC_ETL_JOB.toString).trim
    KAFKA_CONSUMER_GROUP_VMC_WINDOW_JOB = configname.get("consumer.group.vmc.window.job",KAFKA_CONSUMER_GROUP_VMC_WINDOW_JOB.toString).trim
    KAFKA_CONSUMER_GROUP_VMC_INDICATOR_JOB = configname.get("consumer.group.vmc.indicator.job",KAFKA_CONSUMER_GROUP_VMC_INDICATOR_JOB.toString).trim
    KAFKA_CONSUMER_GROUP_VMC_WRITE_INDICATOR_JOB = configname.get("consumer.group.vmc.write.indicator.job",KAFKA_CONSUMER_GROUP_VMC_WRITE_INDICATOR_JOB.toString).trim

    SET_PARALLELISM_VMC_WRITE_INDICATOR_JOB = configname.get("set.parallelism.vmc.write.indicator.job",SET_PARALLELISM_VMC_WRITE_INDICATOR_JOB.toString).trim.toInt
    CHECKPOINT_ENABLE_VMC_WRITE_INDICATOR_JOB = configname.get("checkpoint.enable.vmc.write.indicator.job",CHECKPOINT_ENABLE_VMC_WRITE_INDICATOR_JOB.toString).trim.toBoolean
    CHECKPOINT_STATE_BACKEND_VMC_WRITE_INDICATOR_JOB = configname.get("checkpoint.state.backend.vmc.write.indicator.job",CHECKPOINT_STATE_BACKEND_VMC_WRITE_INDICATOR_JOB).trim
    CHECKPOINT_INTERVAL_VMC_WRITE_INDICATOR_JOB = configname.get("checkpoint.interval.vmc.write.indicator.job",CHECKPOINT_INTERVAL_VMC_WRITE_INDICATOR_JOB.toString).trim.toInt
    CHECKPOINT_TIME_OUT_VMC_WRITE_INDICATOR_JOB = configname.get("checkpoint.time.out.vmc.write.indicator.job",CHECKPOINT_TIME_OUT_VMC_WRITE_INDICATOR_JOB.toString).trim.toInt


    KAFKA_VMC_DATA_TOPIC = configname.get("kafka.vmc.data.topic",KAFKA_VMC_DATA_TOPIC.toString).trim
    KAFKA_VMC_ETL_TOPIC = configname.get("kafka.vmc.etl.topic",KAFKA_VMC_ETL_TOPIC.toString).trim
    KAFKA_VMC_MATCH_CONTROLPLAN_TOPIC = configname.get("kafka.vmc.match.controlplan.topic",KAFKA_VMC_MATCH_CONTROLPLAN_TOPIC.toString).trim
    KAFKA_VMC_ADD_STEP_TOPIC = configname.get("kafka.vmc.add.step.topic",KAFKA_VMC_ADD_STEP_TOPIC.toString).trim
    KAFKA_VMC_INDICATOR_TOPIC = configname.get("kafka.vmc.indicator.topic",KAFKA_VMC_INDICATOR_TOPIC.toString).trim


    // vmc oracle 配置信息
    VMC_ORACLE_URL = configname.get("vmc.oracle.url",VMC_ORACLE_URL.toString).trim
    VMC_ORACLE_USER = configname.get("vmc.oracle.user",VMC_ORACLE_USER.toString).trim
    VMC_ORACLE_PASSWORD = configname.get("vmc.oracle.password",VMC_ORACLE_PASSWORD.toString).trim
    VMC_ORACLE_POOL_MIN_SIZE = configname.get("vmc.oracle.pool.min.size",VMC_ORACLE_POOL_MIN_SIZE.toString).trim.toInt
    VMC_ORACLE_POOL_MAX_SIZE = configname.get("vmc.oracle.pool.max.size",VMC_ORACLE_POOL_MAX_SIZE.toString).trim.toInt

    // 调试输出开关
    VMC_ETL_DEBUG_EANBLE = configname.get("vmc.etl.debug.enable",VMC_ETL_DEBUG_EANBLE.toString).trim.toBoolean
    VMC_MATCH_CONTROLPLAN_DEBUG_EANBLE = configname.get("vmc.match.controlplan.debug.enable",VMC_MATCH_CONTROLPLAN_DEBUG_EANBLE.toString).trim.toBoolean
    VMC_ADD_STEP_DEBUG_EANBLE = configname.get("vmc.add.step.debug.enable",VMC_ADD_STEP_DEBUG_EANBLE.toString).trim.toBoolean
    VMC_INDICATOR_DEBUG_EANBLE = configname.get("vmc.indicator.debug.enable",VMC_INDICATOR_DEBUG_EANBLE.toString).trim.toBoolean

    // Hbase 表
    HBASE_VMC_INDICATOR_DATA_TABLE = configname.get("hbase.vmc.indicator.data.table",HBASE_VMC_INDICATOR_DATA_TABLE.toString).trim
    INDICATOR_BATCH_WRITE_HBASE_NUM = configname.get("indicator.batch.write.hbase.num",INDICATOR_BATCH_WRITE_HBASE_NUM.toString).trim.toInt

    //******************************************************* VMC END ***************************************************




    configname
  }


  /**
   * kafka生产者配置
   *
   * @return
   */
  def getKafkaProperties(): Properties = {
    val properties = new Properties()

    properties.put("bootstrap.servers", KAFKA_QUORUM)
    properties.put("retries", KAFKA_PROPERTIES_RETRIES)
    properties.put("buffer.memory", KAFKA_PROPERTIES_BUFFER_MEMORY)
    properties.put("batch.size", KAFKA_PROPERTIES_BATCH_SIZE)
    properties.put("linger.ms", KAFKA_PROPERTIES_LINGER_MS)
    properties.put("max.in.flight.requests.per.connection", KAFKA_PROPERTIES_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
    properties.put("retry.backoff.ms", KAFKA_PROPERTIES_RETRY_BACKOFF_MS)
    properties.put("max.request.size", KAFKA_PROPERTIES_MAX_REQUEST_SIZE)
    properties.setProperty("transaction.timeout.ms", KAFKA_PROPERTIES_TRANSCATION_TIMEOUT_MS)
    properties.put("compression.type", KAFKA_PROPERTIES_COMPRESSION_TYPE)

    properties
  }


  /**
   * kafka消费者配置
   *
   * @return mn
   */
  def getKafkaConsumerProperties(ip: String, consumer_group: String, autoOffsetReset: String): Properties = {
    val properties = new java.util.Properties()
    properties.setProperty("bootstrap.servers", ip)
    properties.setProperty("group.id", consumer_group)
    properties.setProperty("enable.auto.commit", "true")
    properties.setProperty("auto.commit.interval.ms", "5000")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    /**
     * earliest
     * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
     * latest
     * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
     * none
     * topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
     */

    properties.setProperty("auto.offset.reset", autoOffsetReset)

    properties
  }


}