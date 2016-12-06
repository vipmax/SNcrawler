package org.itmo.escience.core.osn.twitter.tasks

import com.mongodb.BasicDBObject
import org.itmo.escience.core.osn.common.{State, TwitterTask}
import org.itmo.escience.dao.SaverInfo
import twitter4j.Twitter

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterFollowersTask(profileId: Any, saverInfo: SaverInfo)(implicit app: String) extends TwitterTask with State {

  /* state */
  var offset = -1L
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

      logger.debug(localOffset)

      val followers = profileId match {
        case id: String =>
          logger.debug("FollowersCount = " + twitter.showUser(id).getFollowersCount)
          twitter.friendsFollowers.getFollowersIDs(id, localOffset)
        case id: Long =>
          logger.debug("FollowersCount = " + twitter.showUser(id).getFollowersCount)
          twitter.friendsFollowers.getFollowersIDs(id, localOffset)
      }
      _newRequestsCount += 1

      logger.debug(s"followers length = ${followers.getIDs.length} Limits = ${followers.getRateLimitStatus}")

      val data = followers.getIDs.map{ id => new BasicDBObject()
        .append("key", s"${profileId}_$id")
        .append("profile", profileId)
        .append("follower", id)
      }

      Option(saver) match {
        case Some(s) => data.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

      localOffset = followers.getNextCursor
      if(!followers.hasNext) end = true

      saveState(Map("offset" -> localOffset))
    }
  }

  override def name: String = s"TwitterFollowersTask(profileId=$profileId)"

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
