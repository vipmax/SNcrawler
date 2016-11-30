package org.itmo.escience.core.osn.twitter.tasks

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import org.itmo.escience.core.osn.common.{State, TwitterTask}
import org.itmo.escience.dao.SaverInfo
import twitter4j.{Paging, Twitter, TwitterObjectFactory}

import collection.JavaConversions._

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterPostsTask(profileId: Any, saverInfo: SaverInfo)(implicit app: String) extends TwitterTask with State{

  override def name: String = s"TwitterPostsTask(profileId=$profileId)"
  override def appname: String = app

  override def run(network: AnyRef) {
    network match {
      case twitter: Twitter => extractPosts(twitter)
      case _ => logger.debug("No TwitterTemplate object found")
    }
  }


  def extractPosts(twitter: Twitter) {

    var end = false
    var offset = 1
    val maxPostsCount = 20

    while (!end) {

      val paging = new Paging(offset)
      logger.debug(paging)

      val statuses = profileId match {
        case id: String =>
          logger.debug("StatusesCount = "+twitter.showUser(id).getStatusesCount)
          twitter.timelines().getUserTimeline(id, paging)
        case id: Long =>
          logger.debug("StatusesCount = "+twitter.showUser(id).getStatusesCount)
          twitter.timelines().getUserTimeline(id, paging)
      }

      val jsonStatuses = statuses.map{ status =>
          val json = TwitterObjectFactory.getRawJSON(status)
          val basicDBObject = JSON.parse(json).asInstanceOf[BasicDBObject]
          basicDBObject.append("key", s"${basicDBObject.getString("id")}")
      }

      Option(saver) match {
        case Some(s) => jsonStatuses.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

      offset += 1
      if(jsonStatuses.length < maxPostsCount) end = true

      saveState(Map("offset" -> offset))
    }
  }

  /* state */
  var offset = -1L

  override def saveState(stateParams: Map[String, Any]) {
    logger.debug(s"Saving state. stateParams=$stateParams")
    val offset = stateParams.getOrElse("offset", -1).toString.toLong
    this.offset = offset
  }
}
