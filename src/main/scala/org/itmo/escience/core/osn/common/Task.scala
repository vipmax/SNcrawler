package org.itmo.escience.core.osn.common

import org.apache.log4j.Logger
import org.itmo.escience.dao.{Saver, SaverInfo}


/**
  * Created by vipmax on 10.08.16.
  **/


trait Task {
  var logger: Logger = null

  def saverInfo: SaverInfo
  var saver: Saver = null
  var saver2: Saver = null

  def run(network: AnyRef)
  def appname: String
  def name: String
  def taskType() = this.getClass.getSimpleName
}

trait TwitterTask extends Task {
}

trait VkontakteTask extends Task {
}

trait State {
  def saveState(stateParams: Map[String, Any])
}
