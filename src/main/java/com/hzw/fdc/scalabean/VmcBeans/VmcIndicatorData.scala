package com.hzw.fdc.scalabean.VmcBeans

/**
 *
 * @author tanghui
 * @date 2023/9/14 15:47
 * @description VmcIndicatorData
 */
case class VmcIndicatorData(traceId:String,
                            runStartTime:Long,
                            runEndTime:Long,
                            controlPlanId:String,
                            lotName:String,
                            waferName:String,
                            toolName:String,
                            chamberName:String,
                            module:String,
                            recipeName:String,
                            productName:String,
                            stageName:String,
                            route:String,
                            stepId:Long,
                            indexStart:Long,
                            indextEnd:Long,
                            sensorValueClob:String,
                            processTimeStart:Long,
                            processTimeEnd:Long,
                            createTime:String,
                            processDate:String,
                            isrisk:Long,
                            remark:String,
                            lotType:String,
                            owningSite:String,
                            currentSite:String)
