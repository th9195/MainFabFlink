package com.hzw.fdc.function.online.vmc.all

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean}
import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.VmcBeans.{ VmcControlPlanConfig, VmcEventData, VmcEventDataMatchControlPlan, VmcLot, VmcRawData, VmcRawDataMatchedControlPlan}
import com.hzw.fdc.util.{ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions.iterableAsScalaIterable



class VmcAllMatchControlPlanProcessFunction() extends KeyedProcessFunction[String, JsonNode,  JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcAllMatchControlPlanProcessFunction])

  private var vmcEventDataMatchControlPlanListState: ListState[VmcEventDataMatchControlPlan] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // todo 初始化oracle
    logger.error(s"connect oracle start.................")
    val oracleConnectionOk = VmcOracleUtil.getConnection()
    logger.error(s"connect oracle end.................")
    if(!oracleConnectionOk){
      logger.error(s"getOracleConnection error!")
      System.exit(0)
    }

    // 26小时过期
    val hour26TTLConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    val vmcEventDataMatchControlPlanListStateDescription: ListStateDescriptor[VmcEventDataMatchControlPlan] = new
        ListStateDescriptor[VmcEventDataMatchControlPlan]("vmcEventDataMatchControlPlanListState", TypeInformation.of(classOf[VmcEventDataMatchControlPlan]))

    vmcEventDataMatchControlPlanListStateDescription.enableTimeToLive(hour26TTLConfig)

    vmcEventDataMatchControlPlanListState = getRuntimeContext.getListState(vmcEventDataMatchControlPlanListStateDescription)

  }

  override def processElement(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{
      val dataType= inputValue.findPath(VmcConstants.DATA_TYPE).asText()

      if(dataType == VmcConstants.EVENT_START){
        processEventStart(inputValue,context,collector)
      }else if (dataType == VmcConstants.EVENT_END){
        processEventEnd(inputValue,context,collector)
      }else if (dataType == VmcConstants.RAWDATA){
        processRawData(inputValue,context,collector)
      }

    }catch  {
      case e:Exception => {
        logger.error(s"匹配controlPlan 算子 解析源数据失败！inputValue == ${inputValue}\n " +
          s"${e.printStackTrace()}")
      }
    }
  }


  /**
   * 处理eventEnd
   *
   * @param inputValue
   * @param context
   * @param collector
   */
  def processEventEnd(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {

    try{
      val eventEndData = toBean[VmcEventData](inputValue)
      collectEventEndData(eventEndData,context,collector)
    }catch{
      case e:Exception => {
        logger.error(s"匹配controlPlan 算子 processEventEnd error ! inputValue == ${inputValue}\n " +
          s"${e.printStackTrace()}")
      }
    }finally {
      clearAllState()
    }
  }

  def processRawData(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {

    try{
      val vmcRawData = toBean[VmcRawData](inputValue)
      collectRawData(vmcRawData,context,collector)
    }catch{
      case e:Exception => {
        logger.error(s"匹配controlPlan 算子 processRawData error ! inputValue == ${inputValue}\n " +
          s"${e.printStackTrace()}")
      }
    }
  }

  /**
   * 处理processStart
   * @param inputValue
   * @param context
   * @param collector
   */
  def processEventStart(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {

    try{
      val eventStartData = toBean[VmcEventData](inputValue)
      val lotMESInfo = eventStartData.lotMESInfo
      if(null != lotMESInfo && lotMESInfo.nonEmpty){
        // 匹配vmcControlPlan
        logger.warn(s"匹配vmcControlPlan  --------1---------")
        matchVmcControlPlan(eventStartData,context,collector)
        // 分发eventStart
        collectEventStartData(eventStartData,context,collector)

      }else{
        logger.error(s"eventStart lotMESInfo is null ! \n " +
          s"eventStartData == ${eventStartData.toJson} ; ")
      }
    }catch {
      case e:Exception => {
        logger.error(s"匹配controlPlan 算子 processEventStart error ! ${e.printStackTrace()}\n " +
          s"${e.printStackTrace()}")
      }
    }


  }

  /**
   * 通过点查oracle的方式匹配controlPlan
   * @param vmcEventData
   * @param context
   * @param collector
   */
  def matchVmcControlPlan(vmcEventData: VmcEventData, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    val toolName = vmcEventData.toolName
    val lotMESInfo = vmcEventData.lotMESInfo
    val recipeName = vmcEventData.recipeName

    lotMESInfo.foreach((lot: Option[VmcLot]) => {
      val lotInfo = lot.get
      val stageName = lotInfo.stage.get
      val productName = lotInfo.product.get
      val route = lotInfo.route.get
      if (!route.isEmpty && !stageName.isEmpty) {
        // todo 点查oracle
        logger.warn(s"匹配vmcControlPlan 点查oracle --------2---------")
        val vmcControlPlanConfigList = VmcOracleUtil.queryControlPlanConfigFromOracle(toolName, recipeName,route,productName, stageName)

        if(vmcControlPlanConfigList.nonEmpty){
          logger.warn(s"匹配vmcControlPlan 点查oracle vmcControlPlanConfigList size == ${vmcControlPlanConfigList.size}")
          vmcControlPlanConfigList.foreach(vmcControlPlanConfig => {
            // todo 匹配上vmcControlPlanConfig
            updateVmcEventDataMatchControlPlanListState(vmcEventData,vmcControlPlanConfig,context,collector)
          })
        }else{
          logger.error(s"没有匹配到controlPlan vmcEventData == ${vmcEventData.toJson}")
        }
      }
    })

  }

  /**
   * 更新状态数据 : vmcEventDataMatchControlPlanListState
   * @param vmcEventData
   * @param vmcControlPlanConfig
   * @param context
   * @param collector
   */
  def updateVmcEventDataMatchControlPlanListState(vmcEventData: VmcEventData,
                                                  vmcControlPlanConfig: VmcControlPlanConfig,
                                                  context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context,
                                                  collector: Collector[JsonNode]) = {
    val vmcEventDataMatchControlPlan = generateVmcEventDataMatchControlPlan(vmcEventData, vmcControlPlanConfig)
    vmcEventDataMatchControlPlanListState.add(vmcEventDataMatchControlPlan)

  }

  /**
   * 生成对象 VmcEventDataMatchControlPlan
   * @param vmcEventData
   * @param vmcControlPlanConfig
   * @return
   */
  def generateVmcEventDataMatchControlPlan(vmcEventData: VmcEventData, vmcControlPlanConfig: VmcControlPlanConfig) = {
    VmcEventDataMatchControlPlan(dataType = vmcEventData.dataType,
      locationName = vmcEventData.locationName,
      moduleName = vmcEventData.moduleName,
      toolName = vmcEventData.toolName,
      chamberName = vmcEventData.chamberName,
      recipeName = vmcEventData.recipeName,
      recipeActual = vmcEventData.recipeActual,
      runStartTime = vmcEventData.runStartTime,
      runEndTime = vmcEventData.runEndTime,
      runId = vmcEventData.runId,
      traceId = vmcEventData.traceId,
      DCType = vmcEventData.DCType,
      dataMissingRatio = vmcEventData.dataMissingRatio,
      timeRange = vmcEventData.timeRange,
      completed = vmcEventData.completed,
      materialName = vmcEventData.materialName,
      materialActual = vmcEventData.materialActual,
      lotMESInfo = vmcEventData.lotMESInfo,
      errorCode = vmcEventData.errorCode,
      vmcControlPlanConfig = vmcControlPlanConfig,
      stepId = -1,
      indexCount = -1)
  }

  /**
   * 生成对象 VmcRawDataMatchedControlPlan
   * @param rawData
   * @param vmcControlPlanConfig
   * @return
   */
  def generateVmcRawDataMatchedControlPlan(rawData: VmcRawData, vmcControlPlanConfig: VmcControlPlanConfig) = {
    VmcRawDataMatchedControlPlan(dataType = rawData.dataType,
      toolName = rawData.toolName,
      chamberName = rawData.chamberName,
      timestamp = rawData.timestamp,
      traceId = rawData.traceId,
      data = rawData.data,
      controlPlanId = vmcControlPlanConfig.controlPlanId)
  }


  /**
   * 输出eventStart
   * @param eventStartData
   * @param context
   * @param collector
   */
  def collectEventStartData(eventStartData: VmcEventData, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    val vmcEventDataMatchControlPlanList: List[VmcEventDataMatchControlPlan] = vmcEventDataMatchControlPlanListState.get.toList
    vmcEventDataMatchControlPlanList.foreach(vmcEventDataMatchControlPlan => {
      collector.collect(beanToJsonNode[VmcEventDataMatchControlPlan](vmcEventDataMatchControlPlan))
    })
  }


  /**
   * 输出rawData
   * @param rawData
   * @param context
   * @param collector
   */
  def collectRawData(rawData: VmcRawData, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    val vmcEventDataMatchControlPlanList: List[VmcEventDataMatchControlPlan] = vmcEventDataMatchControlPlanListState.get.toList
    vmcEventDataMatchControlPlanList.foreach(elem => {
      val vmcControlPlanConfig = elem.vmcControlPlanConfig
      val vmcRawDataMatchedControlPlan = generateVmcRawDataMatchedControlPlan(rawData, vmcControlPlanConfig)

      collector.collect(beanToJsonNode[VmcRawDataMatchedControlPlan](vmcRawDataMatchedControlPlan))
    })
  }

  /**
   * 输出eventEnd
   * @param eventEndData
   * @param context
   * @param collector
   */
  def collectEventEndData(eventEndData: VmcEventData, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {
    val vmcEventDataMatchControlPlanList: List[VmcEventDataMatchControlPlan] = vmcEventDataMatchControlPlanListState.get.toList
    vmcEventDataMatchControlPlanList.foreach(elem => {
      val vmcControlPlanConfig = elem.vmcControlPlanConfig
      val vmcEventDataMatchControlPlan = generateVmcEventDataMatchControlPlan(eventEndData, vmcControlPlanConfig)
      collector.collect(beanToJsonNode[VmcEventDataMatchControlPlan](vmcEventDataMatchControlPlan))
    })
  }

  /**
   * 清理所有的状态变量
   */
  def clearAllState(): Unit = {
    logger.warn(s"clearAllState")
    vmcEventDataMatchControlPlanListState.clear()
  }

}
