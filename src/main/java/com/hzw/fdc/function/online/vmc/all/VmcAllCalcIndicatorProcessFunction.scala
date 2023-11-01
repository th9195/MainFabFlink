package com.hzw.fdc.function.online.vmc.all

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.function.VmcFunction.VmcCalcIndicatorUtils
import com.hzw.fdc.json.JsonUtil.{beanToJsonNode, toBean, toJson, toJsonNode}
import com.hzw.fdc.scalabean.VmcBeans.{VmcEventData, VmcEventDataMatchControlPlan, VmcIndicatorData, VmcLot, VmcRawData, VmcRawDataAddStep, VmcRawDataCache, VmcSensorIndicatorInfo, VmcWafer}
import com.hzw.fdc.util.DateUtils.DateTimeUtil
import com.hzw.fdc.util.{ProjectConfig, VmcConstants}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer


class VmcAllCalcIndicatorProcessFunction() extends KeyedProcessFunction[String, JsonNode,  JsonNode] {

  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcAllCalcIndicatorProcessFunction])

  // 缓存eventStart数据
  private var vmcEventStartDataMatchControlPlanState:ValueState[VmcEventDataMatchControlPlan] = _

  // 缓存 rawData中所有sensor 的数据
  private var vmcRawDataCacheMapState: MapState[Long,VmcRawDataCache] = _

  // 缓存rawData 第一个index
  private var vmcRawDataIndexStartState:ValueState[Long] = _

  // 缓存rawData 最后一个index
  private var vmcRawDataIndexEndState:ValueState[Long] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 获取全局变量
    val p = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    ProjectConfig.getConfig(p)

    // 26小时过期
    val hour26TTLConfig:StateTtlConfig = StateTtlConfig
      .newBuilder(Time.hours(ProjectConfig.RUN_MAX_LENGTH))
      .useProcessingTime()
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      // compact 过程中清理过期的状态数据
      .cleanupInRocksdbCompactFilter(5000)
      .build()

    //   初始化状态变量 processEndEventStartState
    val processEndEventStartStateDescription =
      new  ValueStateDescriptor[VmcEventDataMatchControlPlan]("processEndEventStartState", TypeInformation.of(classOf[VmcEventDataMatchControlPlan]))
    // 设置过期时间
    processEndEventStartStateDescription.enableTimeToLive(hour26TTLConfig)
    vmcEventStartDataMatchControlPlanState = getRuntimeContext.getState(processEndEventStartStateDescription)

    // 初始化 rawDataMapState
    val vmcRawDataCacheMapStateMapStateDescription: MapStateDescriptor[Long,VmcRawDataCache] = new
        MapStateDescriptor[Long,VmcRawDataCache]("vmcRawDataCacheMapState", TypeInformation.of(classOf[Long]),TypeInformation.of(classOf[VmcRawDataCache]))
    // 设置过期时间
    vmcRawDataCacheMapStateMapStateDescription.enableTimeToLive(hour26TTLConfig)
    vmcRawDataCacheMapState = getRuntimeContext.getMapState(vmcRawDataCacheMapStateMapStateDescription)

    val vmcRawDataIndexStartStateDescription: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("vmcRawDataIndexStartState", TypeInformation.of(classOf[Long]))
    vmcRawDataIndexStartStateDescription.enableTimeToLive(hour26TTLConfig)
    vmcRawDataIndexStartState = getRuntimeContext.getState(vmcRawDataIndexStartStateDescription)


    val vmcRawDataIndexEndStateDescription: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("vmcRawDataIndexEndState", TypeInformation.of(classOf[Long]))
    vmcRawDataIndexEndStateDescription.enableTimeToLive(hour26TTLConfig)
    vmcRawDataIndexEndState = getRuntimeContext.getState(vmcRawDataIndexEndStateDescription)

  }

  override def processElement(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{
      val dataType= inputValue.findPath(VmcConstants.DATA_TYPE).asText()
      if(dataType == VmcConstants.EVENT_START){

        // 处理eventStart
        processEventStart(inputValue,context,collector)
      }else if (dataType == VmcConstants.EVENT_END){

        // 处理eventEnd
        processEventEnd(inputValue,context,collector)
      }else if (dataType == VmcConstants.RAWDATA){

        // 处理eventEnd
        processRawData(inputValue,context,collector)
      }

    }catch  {
      case e:Exception => {
        logger.error(s"计算 indicator算子 解析源数据失败！inputvalue == ${inputValue}\n " +
          s"${e.printStackTrace()}")
      }
    }
  }


  def processEventStart(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]) = {

    try{
      val vmcEventDataMatchControlPlan = toBean[VmcEventDataMatchControlPlan](inputValue)
      updateVmcEventDataMatchControlPlanState(vmcEventDataMatchControlPlan)
    }catch {
      case e:Exception => {
        logger.error(s"计算 indicator算子 processEventStart error ! inputValue == ${inputValue}\n " +
          s"${e.printStackTrace()}")
      }
    }

  }



  def processEventEnd(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {
    try{
      val traceId_controlPlanId_stepId = context.getCurrentKey
      val vmcEventEndDataMatchControlPlan = toBean[VmcEventDataMatchControlPlan](inputValue)
      // todo 1- 解析状态变量中数据
      val parseRawDataStartTime = System.currentTimeMillis()
      logger.warn(s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} 从rockDB中获取数据并解析--------- start : ${parseRawDataStartTime} --------")

      val vmcSensorIndicatorInfoMap = parseVmcRawDataCache(traceId_controlPlanId_stepId)

      val parseRawDataEndTime = System.currentTimeMillis()
      logger.warn(s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} 从rockDB中获取数据并解析--------- end : ${parseRawDataEndTime} -------- cost time == ${parseRawDataEndTime - parseRawDataStartTime}")

      // todo 2- 计算indicator
      val calcIndicatorStartTime = System.currentTimeMillis()
      logger.warn(s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} 计算indicator--------- start : ${calcIndicatorStartTime} --------")

      val sensorIndicatorResult = calcIndicator(traceId_controlPlanId_stepId, vmcEventEndDataMatchControlPlan, vmcSensorIndicatorInfoMap)

      val calcIndicatorEndTime = System.currentTimeMillis()
      logger.warn(s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} 计算indicator--------- end : ${calcIndicatorEndTime} -------- cost time == ${calcIndicatorEndTime - calcIndicatorStartTime}")


      // todo 3- 组装结果数据
      collectorIndicatorResult(traceId_controlPlanId_stepId,
        vmcEventEndDataMatchControlPlan,
        sensorIndicatorResult,
        collector)


    }catch {
      case e:Exception => {
        logger.error(s"计算 indicator算子 processEventEnd error ! inputValue == ${inputValue}\n " +
          s"${e.printStackTrace()}")
      }
    }
  }

  def processRawData(inputValue: JsonNode, context: KeyedProcessFunction[String, JsonNode, JsonNode]#Context, collector: Collector[JsonNode]): Unit = {

    try{
      val vmcRawDataAddStep = toBean[VmcRawDataAddStep](inputValue)
      val timestamp = vmcRawDataAddStep.timestamp
      // 组装缓存的数据结构
      val vmcRawDataCache = generateVmcRawDataCache(vmcRawDataAddStep)

      // rawData缓存到状态变量
      updateVmcRawDataCacheMapState(timestamp,vmcRawDataCache)

      // 记录第1个index
      updateVmcRawDataIndexStartState(vmcRawDataCache)

      // 记录最后一个index
      updateVmcRawDataIndexEndState(vmcRawDataCache)
    }catch {
      case e:Exception => {
        logger.error(s"计算 indicator算子 processRawData error ! inputValue == ${inputValue}\n " +
          s"${e.printStackTrace()}")
      }
    }
  }


  def generateVmcSensorIndicatorInfo(sensorValue: Double, stepId: Long, timestamp: Long) = {
    VmcSensorIndicatorInfo(sensorValue = sensorValue,
      stepId = stepId,
      timestamp = timestamp)
  }

  def parseVmcRawDataCache(traceId_controlPlanId_stepId:String) = {

    val it = vmcRawDataCacheMapState.values().iterator()

    val vmcSensorIndicatorInfoMap = TrieMap[String, ListBuffer[VmcSensorIndicatorInfo]]()

    while (it.hasNext){
      val vmcRawDataCache: VmcRawDataCache = it.next()
      val vmcSensorCacheInfoList = vmcRawDataCache.vmcSensorCacheInfoList
      val stepId = vmcRawDataCache.stepId
      val timestamp = vmcRawDataCache.timestamp

      vmcSensorCacheInfoList.foreach(vmcSensorCacheInfo => {
        val sensorAlias = vmcSensorCacheInfo.sensorAlias
        val sensorValue = vmcSensorCacheInfo.sensorValue
        val vmcSensorIndicatorInfo = generateVmcSensorIndicatorInfo(sensorValue, stepId, timestamp)
        val sensorValueList: ListBuffer[VmcSensorIndicatorInfo] = vmcSensorIndicatorInfoMap.getOrElse(sensorAlias,ListBuffer[VmcSensorIndicatorInfo]())
        sensorValueList.append(vmcSensorIndicatorInfo)
        vmcSensorIndicatorInfoMap.put(sensorAlias,sensorValueList)
      })
    }

    logger.warn(s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} parseVmcRawDataCache sensor 个数 vmcSensorIndicatorInfoMap size == ${vmcSensorIndicatorInfoMap.size}")
    vmcSensorIndicatorInfoMap
  }


  def calcIndicator(traceId_controlPlanId_stepId: String,
                    vmcEventEndDataMatchControlPlan:VmcEventDataMatchControlPlan,
                    sensorMap: TrieMap[String, ListBuffer[VmcSensorIndicatorInfo]]) = {

    val vmcControlPlanConfig = vmcEventEndDataMatchControlPlan.vmcControlPlanConfig
    val calcTypeList = vmcControlPlanConfig.calcTypeList

    val sensorIndicatorResult = sensorMap.map(vmcSensorIndicatorInfoElem => {

      val sensorAlias: String = vmcSensorIndicatorInfoElem._1
      val vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo] = vmcSensorIndicatorInfoElem._2
      val sensorValueMap: Map[String, String] = calcTypeList.map(calcType => {
        val sensorCalcTypeValue = VmcCalcIndicatorUtils.vmcCalcIndicator(traceId_controlPlanId_stepId, vmcSensorIndicatorInfoList, calcType)
        calcType -> sensorCalcTypeValue
      }).toMap

      sensorAlias -> sensorValueMap
    }).toMap

    sensorIndicatorResult
  }

  /**
   * 生成对象 : VmcRawDataCache
   * @param vmcRawDataAddStep
   * @return
   */
  def generateVmcRawDataCache(vmcRawDataAddStep: VmcRawDataAddStep) = {

    VmcRawDataCache(stepId = vmcRawDataAddStep.stepId,
      stepName = vmcRawDataAddStep.stepName,
      index = vmcRawDataAddStep.index,
      timestamp = vmcRawDataAddStep.timestamp,
      vmcSensorCacheInfoList = vmcRawDataAddStep.vmcSensorCacheInfoList)

  }

  def generateVmcIndicatorData(vmcLot: VmcLot,
                               waferName: String,
                               runStartTime:Long,
                               vmcEventEndDataMatchControlPlan: VmcEventDataMatchControlPlan,
                               sensorIndicatorResult: Map[String, Map[String, String]]): VmcIndicatorData = {

    val lotName = vmcLot.lotName.get
    val lotType = vmcLot.lotType.get
    val productName = vmcLot.product.get
    val stageName = vmcLot.stage.get

    val vmcControlPlanConfig = vmcEventEndDataMatchControlPlan.vmcControlPlanConfig
    val rawDataRangeU = vmcControlPlanConfig.rawDataRangeU
    val rawDataRangeL = vmcControlPlanConfig.rawDataRangeL
    val indexCount = vmcEventEndDataMatchControlPlan.indexCount
    val isrisk = if(rawDataRangeL <= indexCount && indexCount <= rawDataRangeU ){
      1
    }else{
      0
    }
    val sensorValueClob = toJson(sensorIndicatorResult)

    val indexStart: Long = vmcRawDataIndexStartState.value()
    val indexEnd: Long = vmcRawDataIndexEndState.value()

    VmcIndicatorData(traceId = vmcEventEndDataMatchControlPlan.traceId ,
      runStartTime = runStartTime,
      runEndTime = vmcEventEndDataMatchControlPlan.runEndTime,
      controlPlanId = vmcControlPlanConfig.controlPlanId,
      lotName = lotName,
      waferName = waferName,
      toolName = vmcEventEndDataMatchControlPlan.toolName,
      chamberName = vmcEventEndDataMatchControlPlan.chamberName,
      module = vmcEventEndDataMatchControlPlan.moduleName,
      recipeName = vmcEventEndDataMatchControlPlan.recipeName,
      productName = productName,
      stageName = stageName,
      route = vmcControlPlanConfig.route,
      stepId = vmcEventEndDataMatchControlPlan.stepId,
      indexStart = indexStart,
      indextEnd = indexEnd,
      sensorValueClob = sensorValueClob,
      processTimeStart = System.currentTimeMillis(),
      processTimeEnd = System.currentTimeMillis(),
      createTime = DateTimeUtil.getCurrentTime(),
      processDate = DateTimeUtil.getToday,
      isrisk = isrisk ,
      remark = "",
      lotType = lotType,
      owningSite = waferName(0).toString,
      currentSite = vmcEventEndDataMatchControlPlan.toolName(0).toString)
  }

  def collectorIndicatorResult(traceId_controlPlanId_stepId: String,
                               vmcEventEndDataMatchControlPlan: VmcEventDataMatchControlPlan,
                               sensorIndicatorResult: Map[String, Map[String, String]],
                               collector: Collector[JsonNode]): Unit  = {
    val vmcEventStartDataMatchControlPlan = vmcEventStartDataMatchControlPlanState.value()
    val vmcControlPlanConfig = vmcEventStartDataMatchControlPlan.vmcControlPlanConfig
    val lotMESInfo = vmcEventStartDataMatchControlPlan.lotMESInfo
    val runStartTime = vmcEventStartDataMatchControlPlan.runStartTime
    if(null == lotMESInfo || !lotMESInfo.nonEmpty){
      logger.error(s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; collectorIndicatorResult error ! lotMesInfo 为空！")
    }else{
      lotMESInfo.foreach(optionLot => {
        val vmcLot: VmcLot = optionLot.get
        val route = vmcLot.route.get
        if(vmcControlPlanConfig.route == route){
          val wafers = vmcLot.wafers
          wafers.foreach(optionWafer => {
            val vmcWafer: VmcWafer = optionWafer.get
            val waferName = vmcWafer.waferName.get
            val vmcIndicatorData = generateVmcIndicatorData(vmcLot,
              waferName,
              runStartTime,
              vmcEventEndDataMatchControlPlan,
              sensorIndicatorResult)

            collector.collect(beanToJsonNode[VmcIndicatorData](vmcIndicatorData))
          })
        }
      })
    }
  }

  /**
   * 更新状态变量 : vmcRawDataCacheMapState
   * @param timestamp
   * @param vmcRawDataCache
   */
  def updateVmcRawDataCacheMapState(timestamp: Long, vmcRawDataCache: VmcRawDataCache): Unit = {
    vmcRawDataCacheMapState.put(timestamp,vmcRawDataCache)
  }

  /**
   * 更新状态变量 : vmcRawDataIndexStartState
   * @param vmcRawDataCache
   */
  def updateVmcRawDataIndexStartState(vmcRawDataCache: VmcRawDataCache) = {

    // 只需要缓存第一个 index 即可
    val rawDataIndexStart: Long = vmcRawDataIndexStartState.value()
    if(null == rawDataIndexStart || 0 == rawDataIndexStart){
      vmcRawDataIndexStartState.update(vmcRawDataCache.index)
    }
  }

  /**
   * 更新状态变量 : vmcRawDataIndexEndState
   * @param vmcRawDataCache
   */
  def updateVmcRawDataIndexEndState(vmcRawDataCache: VmcRawDataCache): Unit = {

    // 一直更新，到最后一个index
    vmcRawDataIndexEndState.update(vmcRawDataCache.index)
  }


  /**
   * 更新状态变量 : vmcEventDataMatchControlPlanState
   * @param vmcEventDataMatchControlPlan
   */
  def updateVmcEventDataMatchControlPlanState(vmcEventDataMatchControlPlan: VmcEventDataMatchControlPlan) = {
    vmcEventStartDataMatchControlPlanState.update(vmcEventDataMatchControlPlan)
  }



  /**
   * 清理所有的状态变量
   */
  def clearAllState() = {
    logger.warn(s"clearAllState")
    vmcEventStartDataMatchControlPlanState.clear()
    vmcRawDataCacheMapState.clear()
    vmcRawDataIndexStartState.clear()
    vmcRawDataIndexEndState.clear()
  }
}
