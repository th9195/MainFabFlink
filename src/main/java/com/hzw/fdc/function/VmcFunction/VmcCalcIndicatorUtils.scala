package com.hzw.fdc.function.VmcFunction

import com.hzw.fdc.json.MarshallableImplicits.Marshallable
import com.hzw.fdc.scalabean.VmcBeans.VmcSensorIndicatorInfo
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcCalcIndicatorUtils
 */
object VmcCalcIndicatorUtils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def vmcCalcIndicator(traceId_controlPlanId_stepId : String,
                       vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
                       calcType:String,
                       paramMap:Map[String,String] = null) = {
    calcType.toUpperCase match {
      case VmcIndicatorCalcType.MAX =>
        MAXAlgorithm(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case VmcIndicatorCalcType.MIN =>
        MINAlgorithm(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case VmcIndicatorCalcType.AVG =>
        AVGAlgorithm(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case VmcIndicatorCalcType.MEANT =>
        meanT(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case VmcIndicatorCalcType.STD =>
        STDAlgorithm(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case VmcIndicatorCalcType.SUM =>
        SUMAlgorithm(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case VmcIndicatorCalcType.Q1 =>
        Q1Algorithm(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case VmcIndicatorCalcType.MEDIAN =>
        MEDIANAlgorithm(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case VmcIndicatorCalcType.Q3 =>
        Q3Algorithm(traceId_controlPlanId_stepId,vmcSensorIndicatorInfoList,paramMap)
      case _ =>
        logger.error(s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} 计算indicator VmcCalcIndicatorUtils calcType == ${calcType} error  ")
        ""
    }
  }

  /**
   * 最大值
   * @param traceId_controlPlanId_stepId
   * @param vmcSensorIndicatorInfoList
   * @param paramMap
   * @return
   */
  def MAXAlgorithm(traceId_controlPlanId_stepId:String,
                   vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
                   paramMap:Map[String,String]): String = {
    try {
      val sensorValueList = for (sensorData <- vmcSensorIndicatorInfoList) yield {
        sensorData.sensorValue
      }
      if(sensorValueList.nonEmpty){
        sensorValueList.max.toString
      }else{
        logger.warn(s"MAXAlgorithm 计算源数据长度为 0 traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId}")
        ""
      }
    } catch {
      case ex: Exception => logger.error(s"MAXAlgorithm error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }

  /**
   * 最小值
   * @param traceId_controlPlanId_stepId
   * @param vmcSensorIndicatorInfoList
   * @param paramMap
   * @return
   */
  def MINAlgorithm(traceId_controlPlanId_stepId:String,
                   vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
                   paramMap:Map[String,String]): String = {
    try {
      val sensorValueList = for (sensorData <- vmcSensorIndicatorInfoList) yield {
        sensorData.sensorValue
      }
      if(sensorValueList.nonEmpty){
        sensorValueList.min.toString
      }else{
        logger.warn(s"MINAlgorithm 计算源数据长度为 0 traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId}")
        ""
      }
    } catch {
      case ex: Exception => logger.error(s"MINAlgorithm error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }

  /**
   * 平均值
   * @param traceId_controlPlanId_stepId
   * @param vmcSensorIndicatorInfoList
   * @param paramMap
   * @return
   */
  def AVGAlgorithm(traceId_controlPlanId_stepId:String,
                   vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
                   paramMap:Map[String,String]): String = {
    try {
      val sensorValueList = for (vmcSensorIndicatorInfo <- vmcSensorIndicatorInfoList) yield {
        vmcSensorIndicatorInfo.sensorValue
      }

      val size = sensorValueList.size
      if(0 < size){
        val avg = sensorValueList.sum/size
          avg.toString
      }else{
        logger.warn(s"AVGAlgorithm 计算源数据长度为 0 traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId}")
        ""
      }

    } catch {
      case ex: Exception => logger.error(s"AVGAlgorithm error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }


  /**
   * 平均值
   * @param traceId_controlPlanId_stepId
   * @param vmcSensorIndicatorInfoList
   * @param paramMap
   * @return
   */
  def SUMAlgorithm(traceId_controlPlanId_stepId:String,
                   vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
                   paramMap:Map[String,String]): String = {
    try {
      val sensorValueList = for (vmcSensorIndicatorInfo <- vmcSensorIndicatorInfoList) yield {
        vmcSensorIndicatorInfo.sensorValue
      }

      val size = sensorValueList.size
      if(0 < size){
        val sum = sensorValueList.sum
        sum.toString
      }else{
        logger.warn(s"SUMAlgorithm 计算源数据长度为 0 traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId}")
        ""
      }

    } catch {
      case ex: Exception => logger.error(s"SUMAlgorithm error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }


  /**
   * 计算 STD
   * @param data
   * @return
   */
  def STDAlgorithm(traceId_controlPlanId_stepId:String,
                   vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
                   paramMap:Map[String,String]): String = {
    try {
      val size = vmcSensorIndicatorInfoList.size
      var sum1 = BigDecimal.valueOf(0d)
      for (n <- vmcSensorIndicatorInfoList) {
        sum1 += BigDecimal(n.sensorValue)
      }
      val avg = sum1 / size
      var sum = BigDecimal.valueOf(0d)

      for (d <- vmcSensorIndicatorInfoList) {
        sum += (BigDecimal(d.sensorValue) - avg).pow(2)
      }

      val variance = sum/size
      val STD = Math.sqrt(variance.toDouble)

      STD.toString
    } catch {
      case ex: Exception => logger.error(s"STDAlgorithm error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }

  /**
   * meanT
   * @param runId
   * @param dataPoints
   * @return
   */
  def meanT(traceId_controlPlanId_stepId:String,
            vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
            paramMap:Map[String,String]): String = {
    try {
      if (vmcSensorIndicatorInfoList.size < 2) throw new RuntimeException("Length Must Greater Than 2")
//      val sortList = vmcSensorIndicatorInfoList.sortBy(elem => {
//        elem.timestamp
//      })
      var t_i = 0L
      var v_i = BigDecimal.valueOf(0d)
      var sum = BigDecimal.valueOf(0d)
      var firstTime = vmcSensorIndicatorInfoList.head.timestamp
      var firstValue = BigDecimal.valueOf(vmcSensorIndicatorInfoList.head.sensorValue)

      for(elem <- vmcSensorIndicatorInfoList.tail){
        t_i = elem.timestamp
        v_i = elem.sensorValue
        sum = sum + ((v_i + firstValue) * (t_i - firstTime)/1000.00) / 2

        firstTime = t_i
        firstValue = v_i
      }
      val res = sum / ((t_i - vmcSensorIndicatorInfoList.head.timestamp)/1000.00)
      res.toString
    }catch {
      case ex: Exception => logger.error(s"meanT error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }


  /**
   * Q1Algorithm
   * 1- 长度必须大于3 ; length >= 3
   * 2- 求位置
   *    低位: (length+1)/4.0  求floor
   *    高位: (length+1)/4.0  求ceil
   * 3- Q1 = 低位上的值 * 0.25 + 高位上的值 * 0.75
   * @param runId
   * @param dataPoints
   * @return
   */
  def Q1Algorithm(traceId_controlPlanId_stepId:String,
            vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
            paramMap:Map[String,String]): String = {
    try {
      if (vmcSensorIndicatorInfoList.size < 3) {
        throw new RuntimeException("Length Must Greater Than 4")
      }else{

        val sortedValueList: ListBuffer[Double] = vmcSensorIndicatorInfoList.map(elem => {
          elem.sensorValue
        }).sorted

        val length = sortedValueList.size
        val location: Double = (length+1)/4.0
        val locationCeil = location.ceil
        val locationFloor = location.floor
        val valueFloor = sortedValueList(locationFloor.toInt - 1 )
        val valueCeil = sortedValueList(locationCeil.toInt - 1 )

        val Q1: Double = valueFloor * 0.25 + valueCeil * 0.75

        Q1.toString
      }
    }catch {
      case ex: Exception => logger.error(s"Q1Algorithm error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }


  /**
   * MEDIANAlgorithm
   * 1- 长度必须大于3 ; length >= 3
   * 2- 求位置
   *    低位: 2*(length+1)/4.0  求floor
   *    高位: 2*(length+1)/4.0  求ceil
   * 3- Q3 = 低位上的值 * 0.5 + 高位上的值 * 0.5
   * @param runId
   * @param dataPoints
   * @return
   */
  def MEDIANAlgorithm(traceId_controlPlanId_stepId:String,
                  vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
                  paramMap:Map[String,String]): String = {
    try {
      if (vmcSensorIndicatorInfoList.size < 3) {
        throw new RuntimeException("Length Must Greater Than 4")
      }else{

        val sortedValueList: ListBuffer[Double] = vmcSensorIndicatorInfoList.map(elem => {
          elem.sensorValue
        }).sorted

        val length = sortedValueList.size
        val location: Double = 2*(length+1)/4.0
        val locationCeil = location.ceil
        val locationFloor = location.floor
        val valueFloor = sortedValueList(locationFloor.toInt - 1 )
        val valueCeil = sortedValueList(locationCeil.toInt - 1 )

        val median: Double = valueFloor * 0.5 + valueCeil * 0.5

        median.toString
      }
    }catch {
      case ex: Exception => logger.error(s"MEDIANAlgorithm error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }


  /**
   * Q3Algorithm
   * 1- 长度必须大于3 ; length >= 3
   * 2- 求位置
   *    低位: 3*(length+1)/4.0  求floor
   *    高位: 3*(length+1)/4.0  求ceil
   * 3- Q3 = 低位上的值 * 0.75 + 高位上的值 * 0.25
   * @param runId
   * @param dataPoints
   * @return
   */
  def Q3Algorithm(traceId_controlPlanId_stepId:String,
                  vmcSensorIndicatorInfoList: ListBuffer[VmcSensorIndicatorInfo],
                  paramMap:Map[String,String]): String = {
    try {
      if (vmcSensorIndicatorInfoList.size < 3) {
        throw new RuntimeException("Length Must Greater Than 4")
      }else{

        val sortedValueList: ListBuffer[Double] = vmcSensorIndicatorInfoList.map(elem => {
          elem.sensorValue
        }).sorted

        val length = sortedValueList.size
        val location: Double = 3*(length+1)/4.0
        val locationCeil = location.ceil
        val locationFloor = location.floor
        val valueFloor = sortedValueList(locationFloor.toInt - 1 )
        val valueCeil = sortedValueList(locationCeil.toInt - 1 )

        val Q3: Double = valueFloor * 0.75 + valueCeil * 0.25

        Q3.toString
      }
    }catch {
      case ex: Exception => logger.error(s"Q3Algorithm error: ${ex.printStackTrace()}\n " +
        s"traceId_controlPlanId_stepId == ${traceId_controlPlanId_stepId} ; \n" +
        s"vmcSensorIndicatorInfoList == ${vmcSensorIndicatorInfoList.toJson}" )
        ""
    }
  }

}
