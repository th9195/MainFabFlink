package com.hzw.fdc.controller.Vmc

import com.hzw.fdc.common.TController
import com.hzw.fdc.service.Vmc.{VmcAllService, VmcWriteIndicatorService}

/**
 *
 * @author tanghui
 * @date 2023/8/26 11:04
 * @description VmcWriteIndicatorController
 */
class VmcWriteIndicatorController extends TController {
  private val service = new VmcWriteIndicatorService

  override def execute(): Unit = service.analyses()
}
