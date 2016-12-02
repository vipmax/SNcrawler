package org.itmo.escience.core.osn.twitter.tasks

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import org.itmo.escience.core.osn.common.TwitterTask
import org.itmo.escience.dao.SaverInfo
import twitter4j.{Twitter, TwitterObjectFactory, User}

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterProfileTask(profileId: Any, saverInfo: SaverInfo)(implicit app: String) extends TwitterTask {

  override def appname: String = app

  override def run(network: AnyRef) {
    network match {
      case twitter: Twitter => extract(twitter)
      case _ => logger.debug("No TwitterTemplate object found")
    }
  }

  def extract(twitter: Twitter) {
    val userProfile = profileId match {
      case id: String =>
        twitter.showUser(id)
      case id: Long =>
        twitter.showUser(id)
    }

    logger.debug("userProfile = " + userProfile)
    val data = getData(userProfile)

    Option(saver) match {
      case Some(s) => s.save(data)
      case None => logger.debug(s"No saver for task $name")
    }
  }

  override def name: String = s"TwitterProfileTask(profileId=$profileId)"

  private def getData(userProfile: User) = {
    val json = TwitterObjectFactory.getRawJSON(userProfile)
    val basicDBObject = JSON.parse(json).asInstanceOf[BasicDBObject]
    basicDBObject.append("key", s"${basicDBObject.getString("id")}")
  }

  override def newRequestsCount() = 1
}
