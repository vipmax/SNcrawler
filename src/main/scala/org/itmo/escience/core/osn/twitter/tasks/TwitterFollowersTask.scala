package org.itmo.escience.core.osn.twitter.tasks

import akka.actor.ActorRef
import com.mongodb.BasicDBObject
import org.itmo.escience.core.osn.common.{State, TwitterTask}
import org.itmo.escience.dao.SaverInfo
import twitter4j.Twitter

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterFollowersTaskResponse(profileId: Any, followers: Array[Long])

case class TwitterFollowersTask(profileId: Any,
                                var count: Int = 5000,
                                responseActor: ActorRef = null,
                                saverInfo: SaverInfo)(implicit app: String) extends TwitterTask with State {

  /* state */
  var offset = -1L
  var _newRequestsCount = 0

  override def appname: String = app

  override def run(network: AnyRef) {
    logger.debug(s"Starting  $name task")
    network match {
      case twitter: Twitter => extract(twitter)
      case _ => logger.debug("No TwitterTemplate object found")
    }
  }

  def extract(twitter: Twitter) {
    var end = false
    /* read state */
    var localOffset = offset

    var followersCount = profileId match {
      case id: String => twitter.showUser(id).getFollowersCount
      case id: Long =>   twitter.showUser(id).getFollowersCount
      case id: Int =>    twitter.showUser(id).getFollowersCount
    }

    while (!end) {
      val followers = profileId match {
        case id: String => twitter.friendsFollowers.getFollowersIDs(id, localOffset)
        case id: Long =>   twitter.friendsFollowers.getFollowersIDs(id, localOffset)
        case id: Int =>    twitter.friendsFollowers.getFollowersIDs(id, localOffset)
      }
      _newRequestsCount += 1
      count -= followers.getIDs.length

      logger.debug(s"Got  = ${followers.getIDs.length} followers (remainig $count/$followersCount) RateLimitStatus= ${followers.getRateLimitStatus}")

      val data: Array[BasicDBObject] = followers.getIDs.map{ id => new BasicDBObject()
        .append("key", s"${profileId}_$id")
        .append("profile", profileId)
        .append("follower", id)
      }

      Option(saver) match {
        case Some(s) => data.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }
      Option(responseActor) match {
        case Some(actor) => actor ! TwitterFollowersTaskResponse(profileId, followers.getIDs)
        case None => logger.debug(s"No response Actor for task $name")
      }

      localOffset = followers.getNextCursor
      if(!followers.hasNext || count <= 0) end = true

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
