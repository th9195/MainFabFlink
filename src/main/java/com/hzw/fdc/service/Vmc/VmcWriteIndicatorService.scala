package com.hzw.fdc.service.Vmc

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.common.{TDao, TService}
import com.hzw.fdc.dao.Vmc.VmcWriteIndicatorDao
import com.hzw.fdc.function.online.vmc.writeIndicator.{VmcIndicatorWriteCountTrigger, VmcWriteIndicatorHbaseSink}
import com.hzw.fdc.util.{ProjectConfig, VmcConstants}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}



/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWriteIndicatorService
 */
class VmcWriteIndicatorService extends TService {
  private val dao = new VmcWriteIndicatorDao
  private val logger: Logger = LoggerFactory.getLogger(classOf[VmcWriteIndicatorService])

  /**
   * 获取
   *
   * @return
   */
  override def getDao(): TDao = dao

  /**
   * 分析
   *
   * @return
   */
  override def analyses(): Any = {
    // todo 源数据流
    val sourceDS = getDatas()

    val vmcIndicatorListDataStream: DataStream[List[JsonNode]] = sourceDS
      .keyBy(
        data => {
          val toolName = data.findPath(VmcConstants.TOOL_NAME).asText()
          toolName
        }
      )
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .trigger(new VmcIndicatorWriteCountTrigger())
      .process(new VmcIndicatorResultProcessWindowFunction())


    vmcIndicatorListDataStream.addSink(new VmcWriteIndicatorHbaseSink())
      .name("vmcIndicator sink to Hbase")
      .uid("vmcIndicator sink to Hbase")

  }

  /**
   *  聚合在一起，批量写入
   */
  class VmcIndicatorResultProcessWindowFunction extends ProcessWindowFunction[JsonNode, List[JsonNode], String,
    TimeWindow] {

    def process(key: String, context: Context, input: Iterable[JsonNode],
                out: Collector[List[JsonNode]]): Unit = {
      try {
        if(input.nonEmpty){
          out.collect(input.toList)
        }
      }catch {
        case ex: Exception => logger.warn(s"VmcIndicatorResultProcessWindowFunction error: ${ex.toString}")
      }
    }
  }

  /**
   * 获取Indicator和增量IndicatorRule数据
   */
  override protected def getDatas(): DataStream[JsonNode] = {
    getDao().getKafkaJsonSource(
      ProjectConfig.KAFKA_VMC_INDICATOR_TOPIC,
      ProjectConfig.KAFKA_QUORUM,
      ProjectConfig.KAFKA_CONSUMER_GROUP_VMC_WRITE_INDICATOR_JOB,
      VmcConstants.latest,
      VmcConstants.VMC_WRITE_INDICATOR_JOB_KAFKA_SOURCE_UID,
      VmcConstants.VMC_WRITE_INDICATOR_JOB_KAFKA_SOURCE_UID)

  }

}
