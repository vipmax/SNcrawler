package org.itmo.escience.core.osn.common

import org.apache.log4j.Logger
import org.itmo.escience.dao.{Saver, SaverInfo}


/**
  * Created by vipmax on 10.08.16.
  **/


trait Task {
  var logger: Logger = null
  var saver: Saver = null
  var saver2: Saver = null

  def saverInfo: SaverInfo

  def run(network: AnyRef)
  def appname: String
  def name: String
  def taskType() = this.getClass.getSimpleName
}

trait TwitterTask extends Task {
  def newRequestsCount(): Int
}

trait VkontakteTask extends Task {
}

trait InstagramTask extends Task {
}

trait State {
  def saveState(stateParams: Map[String, Any])
}
