package com.hzw.fdc.scalabean.VmcBeans

import scala.beans.BeanProperty

/**
 *
 * @author tanghui
 * @date 2023/9/5 10:40
 * @description VmcRawData
 */
case class VmcRawData(dataType: String,
                      toolName: String,
                      chamberName: String,
                      timestamp: Long,
                      traceId: String,
                      @BeanProperty var data: List[VmcSensorData])


case class VmcSensorData(svid: String,
                        sensorName: String,
                        sensorAlias: String,
                        sensorValue: Any,
                        unit: String)


case class VmcRawDataMatchedControlPlan(dataType: String,
                                        toolName: String,
                                        chamberName: String,
                                        timestamp: Long,
                                        traceId: String,
                                        @BeanProperty var data: List[VmcSensorData],
                                        controlPlanId : String)


case class VmcRawDataAddStep(dataType: String,
                             toolName: String,
                             chamberName: String,
                             timestamp: Long,
                             traceId: String,
                             index:Long,
                             vmcSensorCacheInfoList: List[VmcSensorCacheInfo],
                             controlPlanId : String,
                             stepId: Long,
                             stepName: String)


case class VmcSensorCacheInfo(sensorAlias:String,
                              sensorValue:Double,
                              unit:String)

case class VmcRawDataCache(stepId:Long,
                           stepName: String,
                           index:Long,
                           timestamp:Long,
                           vmcSensorCacheInfoList: List[VmcSensorCacheInfo])


case class VmcSensorIndicatorInfo(sensorValue:Double,
                                  stepId:Long,
                                  timestamp:Long)




