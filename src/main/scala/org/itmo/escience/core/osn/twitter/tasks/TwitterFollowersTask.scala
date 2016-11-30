package org.itmo.escience.core.osn.twitter.tasks

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import org.itmo.escience.core.osn.common.{State, TwitterTask}
import org.itmo.escience.dao.SaverInfo
import twitter4j.{Paging, Twitter, TwitterObjectFactory}

import scala.collection.JavaConversions._

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterFollowersTask(profileId: Any, saverInfo: SaverInfo)(implicit app: String) extends TwitterTask with State {

  override def name: String = s"TwitterFollowersTask(profileId=$profileId)"
  override def appname: String = app

  override def run(network: AnyRef) {
    network match {
      case twitter: Twitter => extractPosts(twitter)
      case _ => logger.debug("No TwitterTemplate object found")
    }
  }

  def extractPosts(twitter: Twitter) {
    var end = false
    var offset = -1L
    val maxPostsCount = 20

    while (!end) {

      logger.debug(offset)

      val followers = profileId match {
        case id: String =>
          logger.debug("FollowersCount = " + twitter.showUser(id).getFollowersCount)
          twitter.friendsFollowers.getFollowersIDs(id, offset)
        case id: Long =>
          logger.debug("FollowersCount = " + twitter.showUser(id).getFollowersCount)
          twitter.friendsFollowers.getFollowersIDs(id, offset)
      }

      logger.debug("followers lenght = " + followers.getIDs.length)

      val data = followers.getIDs.map{ id => new BasicDBObject()
        .append("key", s"${profileId}_$id")
        .append("profile", profileId)
        .append("follower", id)
      }

      Option(saver) match {
        case Some(s) => data.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

      offset = followers.getNextCursor
      if(!followers.hasNext) end = true

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
