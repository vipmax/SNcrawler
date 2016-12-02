package org.itmo.escience.core.osn.twitter.tasks

import org.itmo.escience.core.osn.common.{State, TwitterTask}
import org.itmo.escience.dao.SaverInfo
import twitter4j._

import scala.collection.JavaConversions._

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterPostsTask(profileId: Any, saverInfo: SaverInfo)(implicit app: String) extends TwitterTask with State{

  /* state */
  var offset = 1L
  var _newRequestsCount = 0

  override def appname: String = app

  override def run(network: AnyRef) {
    network match {
      case twitter: Twitter => extract(twitter)
      case _ => logger.debug("No TwitterTemplate object found")
    }
  }

  def extract(twitter: Twitter) {
    var end = false
    /* read state */
    var localOffset = offset
    val maxPostsCount = 20

    while (!end) {

      val paging = new Paging(localOffset)
      logger.debug(paging)

      val statuses = profileId match {
        case id: String =>
          //          logger.debug("StatusesCount = "+twitter.showUser(id).getStatusesCount)
          twitter.timelines().getUserTimeline(id, paging)
        case id: Long =>
          //          logger.debug("StatusesCount = "+twitter.showUser(id).getStatusesCount)
          twitter.timelines().getUserTimeline(id, paging)
      }
      _newRequestsCount += 1

      val data = TwitterTaskUtil.mapStatuses(statuses.toList)

      Option(saver) match {
        case Some(s) => data.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

      localOffset += 1
      if (data.length < maxPostsCount) end = true

      saveState(Map("offset" -> localOffset))
    }
  }

  override def name: String = s"TwitterPostsTask(profileId=$profileId)"

  override def saveState(stateParams: Map[String, Any]) {
    logger.debug(s"Saving state. stateParams=$stateParams")
    val offset = stateParams.getOrElse("offset", -1).toString.toLong
    this.offset = offset
  }

  override def newRequestsCount() = {
    val returned = _newRequestsCount
    _newRequestsCount = 0
    returned
  }
}
