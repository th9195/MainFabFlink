package com.hzw.fdc.scalabean.VmcBeans

import scala.collection.concurrent.TrieMap

/**
 *
 * @author tanghui
 * @date 2023/9/5 10:04
 * @description VmcConfigData
 */
case class VmcConfig[T](`dataType`:String,
                          serialNo:String,
                          timestamp:Long,
                          status:String,
                          data:T)

//case class VmcControlPlanConfig(controlPlanId:String,
//                                toolName:String,
//                                recipeSubName:String,
//                                stageName:String,
//                                route:String,
//                                vmcSensorInfoList:List[VmcSensorInfo],
//                                calcTypeList:List[String],
//                                windowType:String,
//                                rawDataRangeU:Long,
//                                rawDataRangeL:Long)


case class VmcControlPlanConfig(controlPlanId:String,
                                toolName:String,
                                recipeSubName:String,
                                recipeName:String,
                                productName:String,
                                stageName:String,
                                route:String,
                                vmcSensorConfigMap:Map[String,String],
                                calcTypeList:List[String],
                                windowType:String,
                                rawDataRangeU:Long,
                                rawDataRangeL:Long)

