package com.hzw.fdc.function.online.vmc.all

import com.hzw.fdc.scalabean.VmcBeans.VmcControlPlanConfig
import com.hzw.fdc.util.{ExceptionInfo, ProjectConfig}
import jodd.util.StringUtil
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.mutable.ListBuffer

/**
 *
 * @author tanghui
 * @date 2023/9/16 17:30
 * @description NewOracleUtil
 */
object VmcOracleUtil {
  private val logger: Logger = LoggerFactory.getLogger(OracleUtil.getClass)
  var conn: Connection = _

  def getConnection() = {

    try{
      Class.forName("oracle.jdbc.driver.OracleDriver")
      conn = DriverManager.getConnection(ProjectConfig.VMC_ORACLE_URL,
        ProjectConfig.VMC_ORACLE_USER, ProjectConfig.VMC_ORACLE_PASSWORD)

      true
    }catch {
      case e:Exception => {
        logger.error(s"${e.printStackTrace()}")
        false
      }
    }
  }

  def colse() = {
    if(null != conn){
      conn.close()
    }
  }

  def reConnection() = {
    colse()
    getConnection()
  }


  /**
   * 用于第一次点查根据 toolName 过滤
   * @param toolName
   * @return
   */
  def queryControlPlanCountByToolName(toolName:String): Boolean ={
    var res = false

    var prepareStatement: PreparedStatement = null

    val sql =
      s"""
         |select
         |  count(1) as COUNT
         |from modelalgo_fdc_feature_eqp
         |where eqp_id = '${toolName}'
         |""".stripMargin

    try{
      if(null == conn){
        getConnection()
      }

      prepareStatement = conn.prepareStatement(sql)
      val result = prepareStatement.executeQuery()
      while (result.next()) {
        val count = result.getLong("COUNT")
        if(count > 0){
          res = true
        }
      }

    }catch {
      case e:Exception => {
        logger.error(s"queryControlPlanCountByToolName 查询异常！queryControlPlanConfigFromOracle error!")
        logger.error(s"VMC_ORACLE_URL == ${ProjectConfig.VMC_ORACLE_URL}")
        logger.error(s"VMC_ORACLE_USER == ${ProjectConfig.VMC_ORACLE_USER}")
        logger.error(s"VMC_ORACLE_PASSWORD == ${ProjectConfig.VMC_ORACLE_PASSWORD}")
        logger.error(s"点查策略异常:sql == ${sql} ; ")
        logger.error(s"error ==> ${ExceptionInfo.getExceptionInfo(e)}")
        reConnection()
      }
    }finally{
      if(null != prepareStatement){
        prepareStatement.close()
      }
    }
    res
  }


  /**
   * 用于第二次点查，精准查询到匹配上册contorlPlanId
   * @param toolName
   * @param recipeName
   * @param route
   * @param productName
   * @param stageName
   * @return
   */
  def queryControlPlanConfigFromOracle(toolName:String,
                                       recipeName:String,
                                       route:String,
                                       productName:String,
                                       stageName:String) = {

    val controlPlanConfigList = new ListBuffer[VmcControlPlanConfig]()
    var prepareStatement:PreparedStatement = null

    val sql =
      s"""
         |select
         |mff.COL_ID AS COL_ID ,
         |mff.STAGE_ID AS STAGE_ID ,
         |mff.PROCESS_STEPSEQ AS PROCESS_STEPSEQ ,
         |mff.RECIPE_ID AS RECIPE_ID ,
         |mff.SENSOR_LIST AS SENSOR_LIST ,
         |mff.WINDOW_TYPE AS WINDOW_TYPE ,
         |mff.CALC_TYPE AS CALC_TYPE ,
         |mffe.EQP_ID AS EQP_ID,
         |mffc.PARA_VALUE  AS PARA_VALUE
         |from modelalgo_fdc_feature mff,modelalgo_fdc_feature_eqp mffe,modelalgo_fdc_feature_cfg mffc
         |where mff.col_id = mffe.col_id
         |and mff.col_id = mffc.col_id
         |and mffe.eqp_id = '${toolName}'
         |and mff.stage_id = '${stageName}'
         |and mff.process_stepseq = '${route}'
         |and UPPER(mffc.para_name) = 'FILE_COUNT_MIN_MAX'
         |and UPPER(mff.status) = 'ON'
         |and instr('${recipeName}',recipe_id)>0
         |""".stripMargin

    try{

      if(null == conn){
        getConnection()
      }

      prepareStatement = conn.prepareStatement(sql)
      val result = prepareStatement.executeQuery()

      while (result.next()) {
        val controlPlanId = result.getLong("COL_ID")
        val calcType = result.getString("CALC_TYPE")
        val recipeSubName = result.getString("RECIPE_ID")
        val sensorList = result.getString("SENSOR_LIST")
        val windowType = result.getString("WINDOW_TYPE")
        val paraValue = result.getString("PARA_VALUE")

        val calcTypeList = getCalcTypeList(calcType)
        val vmcSensorConfigMap: Map[String, String] = getVmcSensorConfigMap(sensorList)
        val rangUL: (Long, Long) = getRangUL(paraValue)

        if(calcTypeList.nonEmpty && vmcSensorConfigMap.nonEmpty && null != rangUL){
          val vmcControlPlanCofnig = VmcControlPlanConfig(controlPlanId = controlPlanId.toString,
            toolName = toolName,
            recipeSubName = recipeSubName,
            recipeName = recipeName,
            productName = productName,
            stageName = stageName,
            route = route,
            vmcSensorConfigMap = vmcSensorConfigMap,
            calcTypeList = calcTypeList,
            windowType:String,
            rawDataRangeU = rangUL._1,
            rawDataRangeL = rangUL._2)

          controlPlanConfigList.append(vmcControlPlanCofnig)
        }else{
          logger.error(s"点查策略异常:sql == ${sql} ; ")
        }
      }
    }catch {
      case e:Exception => {
        logger.error(s"queryControlPlanConfigFromOracle 查询异常！queryControlPlanConfigFromOracle error!")
        logger.error(s"VMC_ORACLE_URL == ${ProjectConfig.VMC_ORACLE_URL}")
        logger.error(s"VMC_ORACLE_USER == ${ProjectConfig.VMC_ORACLE_USER}")
        logger.error(s"VMC_ORACLE_PASSWORD == ${ProjectConfig.VMC_ORACLE_PASSWORD}")
        logger.error(s"点查策略异常:sql == ${sql} ; ")
        logger.error(s"error ==> ${ExceptionInfo.getExceptionInfo(e)}")
        getConnection()
      }
    }finally {
      if(null != prepareStatement){
        prepareStatement.close()
      }
    }

    controlPlanConfigList
  }


  def getCalcTypeList(calcType: String) = {

    try{
      if(StringUtil.isNotBlank(calcType)){
        calcType.split(",").toList
      }else{
        List[String]()
      }
    }catch {
      case e:Exception => {
        logger.error(s"getCalcTypeList error ! calcType == ${calcType};\n" +
          s"${e.printStackTrace()}")
        List[String]()
      }
    }

  }

  def getVmcSensorConfigMap(sensorList: String): Map[String, String] = {
    try{
      if(StringUtil.isNotBlank(sensorList)){
        sensorList.split(",").toList.map(elem => {
          val sensorElem = elem.split(":").toList
          sensorElem(0) -> sensorElem(1)
        }).toMap
      }else{
        Map[String,String]()
      }
    }catch {
      case e:Exception => {
        logger.error(s"getVmcSensorConfigMap error ! sensorList == ${sensorList} ; \n " +
          s"${e.printStackTrace()}")
        Map[String,String]()
      }
    }
  }


  def getRangUL(paraValue: String): (Long, Long) = {
    try{
      if (StringUtil.isNotBlank(paraValue)) {
        val rangeUL = paraValue.split(":").toList
        (rangeUL(0).toLong, rangeUL(1).toLong)
      } else {
        null
      }
    }catch {
      case e:Exception => {
        logger.error(s"getRangUL error ! paraValue == ${paraValue};\n " +
          s"${e.printStackTrace()}")
        null
      }
    }
  }

}
