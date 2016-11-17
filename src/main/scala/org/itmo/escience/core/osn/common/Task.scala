package org.itmo.escience.core.osn.common


/**
  * Created by vipmax on 10.08.16.
  **/


trait Task {
  def run(network: AnyRef)
  def appname: String
  def name: String
}

trait TaskResult {
  def get()
}

trait TwitterTask extends Task with TaskResult {
}

trait VkontakteTask extends Task with TaskResult {
}

