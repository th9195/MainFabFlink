package com.hzw.fdc.function.online.vmc.writeIndicator

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.toBean
import com.hzw.fdc.scalabean.VmcBeans.VmcIndicatorData
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.slf4j.{Logger, LoggerFactory}


class VmcWriteIndicatorHbaseSink() extends RichSinkFunction[List[JsonNode]] {

  var connection: Connection = _
  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcWriteIndicatorHbaseSink])

  var mutator: BufferedMutator = _
  var count = 0


  override def open(parameters: Configuration): Unit = {
    // 获取全局变量
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(parameters)

    //创建hbase连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ProjectConfig.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", ProjectConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    connection = ConnectionFactory.createConnection(conf)

    /**
     * 连接hbase表
     */
    startHbaseTable()
  }

  /**
   * 每条数据写入
   *
   */
  override def invoke(value: List[JsonNode]): Unit = {
    value.grouped(ProjectConfig.INDICATOR_BATCH_WRITE_HBASE_NUM).foreach(datas => {
      try {
        datas.foreach(invokeIndicatorDataSite)
        mutator.flush()
      }catch {
        case ex: Exception => logger.warn("flush error: " + ex.toString)
          /**
           * 连接hbase表
           */
          startHbaseTable()
      }
    })
  }

  /**
   * 插入hbase
   *
   */
  def invokeIndicatorDataSite(elem: JsonNode): Unit = {
    try {
        val vmcIndicatorData = toBean[VmcIndicatorData](elem)
        val rowkey = generateRowkey(vmcIndicatorData)
        val put = generatePut(rowkey,vmcIndicatorData)
        mutator.mutate(put)
        //每满500条刷新一下数据
        if (count >= ProjectConfig.INDICATOR_BATCH_WRITE_HBASE_NUM){
          mutator.flush()
          count = 0
        }
        count = count + 1
    } catch {
      case ex: Exception => {
        logger.warn(s"VmcIndicatorSinkHbase  invokeIndicatorDataSite Exception:${ExceptionInfo.getExceptionInfo(ex)}")
        // todo 重连Hbase
        startHbaseTable()
      }
    }
  }

  /**
   * Rowkey设计 :
   *    baseKeyInfo = ROUTER反转|WAFER_ID反转
   *    rowkeyMd5 = MD5(baseKeyInfo)
   *    rowkey = rowkeyMd5|stepId
   *
   * @param vmcIndicatorData
   * @return
   */
  def generateRowkey(vmcIndicatorData: VmcIndicatorData) = {
    // rowkey查询的主要字段

    val route = vmcIndicatorData.route
    val waferName = vmcIndicatorData.waferName
    val stepId = vmcIndicatorData.stepId

    val baseRowkeyInfo = s"${route.reverse}|${waferName.reverse}"
    val rowkeyMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(baseRowkeyInfo))

    s"${rowkeyMd5}|${stepId}"
  }

  def generatePut(rowkey: String, vmcIndicatorData: VmcIndicatorData) = {
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("TRACE_ID"), Bytes.toBytes(vmcIndicatorData.traceId.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_EVENT_START_TIME"), Bytes.toBytes(vmcIndicatorData.runStartTime.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RUN_EVENT_END_TIME"), Bytes.toBytes(vmcIndicatorData.runEndTime.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("COL_ID"), Bytes.toBytes(vmcIndicatorData.controlPlanId.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LOT_ID"), Bytes.toBytes(vmcIndicatorData.lotName.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("WAFER_ID"), Bytes.toBytes(vmcIndicatorData.waferName.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("EQP"), Bytes.toBytes(vmcIndicatorData.toolName.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CHAMBER_ID"), Bytes.toBytes(vmcIndicatorData.chamberName.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("MODULE"), Bytes.toBytes(vmcIndicatorData.module.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("RECIPE"), Bytes.toBytes(vmcIndicatorData.recipeName.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PRODUCT_ID"), Bytes.toBytes(vmcIndicatorData.productName.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ROUTE"), Bytes.toBytes(vmcIndicatorData.route.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STAGE"), Bytes.toBytes(vmcIndicatorData.stageName.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STEP_NO"), Bytes.toBytes(vmcIndicatorData.stepId.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDEX_START"), Bytes.toBytes(vmcIndicatorData.indexStart.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("INDEX_END"), Bytes.toBytes(vmcIndicatorData.indextEnd.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("SENSOR_VALUES"), Bytes.toBytes(vmcIndicatorData.sensorValueClob.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PROCESS_TIME_START"), Bytes.toBytes(vmcIndicatorData.processTimeStart.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PROCESS_TIME_END"), Bytes.toBytes(vmcIndicatorData.processTimeEnd.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CREATED_TIME"), Bytes.toBytes(vmcIndicatorData.createTime.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("PROCESS_DATE"), Bytes.toBytes(vmcIndicatorData.processDate.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("IS_RISK"), Bytes.toBytes(vmcIndicatorData.isrisk.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("REMARK"), Bytes.toBytes(vmcIndicatorData.remark.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("LOT_TYPE"), Bytes.toBytes(vmcIndicatorData.lotType.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("OWNING_SITE"), Bytes.toBytes(vmcIndicatorData.owningSite.toString))
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CURRENT_SITE"), Bytes.toBytes(vmcIndicatorData.currentSite))

    put
  }


  /**
   *  hbase表连接
   */
  def startHbaseTable(): Unit ={
    try {
      val tableName: TableName = TableName.valueOf(ProjectConfig.HBASE_VMC_INDICATOR_DATA_TABLE)
      val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
      //设置缓存2m，当达到1m时数据会自动刷到hbase
      params.writeBufferSize(1 * 1024 * 1024) //设置缓存的大小
      mutator = connection.getBufferedMutator(params)
    }catch {
      case ex: Exception => logger.warn("startHbaseTable error: " + ex.toString)
    }
  }

  /**
   * 关闭链接
   */
  override def close(): Unit = {
    if(mutator != null){
      mutator.close()
    }
    if(connection != null){
      connection.close()
    }
  }

}
