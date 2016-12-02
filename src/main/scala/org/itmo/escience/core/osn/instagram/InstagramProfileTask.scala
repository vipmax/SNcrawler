package org.itmo.escience.core.osn.instagram

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.SaverInfo
import org.jsoup.Jsoup

/**
  * Created by vipmax on 31.10.16.
  */
case class InstagramProfileTask(profileId: Any, saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def appname: String = app

  override def run(network: AnyRef) {

    val json = profileId match {
      case username: String =>
        Jsoup.connect(s"https://instagram.com/$username/?__a=1").ignoreContentType(true).execute().body()
      case id: Long =>
        logger.debug(s"Needs to get username by id=$id")
        "not implemented yet"
    }

    val userInfo = JSON.parse(json).asInstanceOf[BasicDBObject].get("user").asInstanceOf[BasicDBObject]
    userInfo.append("key", userInfo.getString("id"))

    Option(saver) match {
      case Some(s) => s.save(userInfo)
      case None => logger.debug(s"No saver for task $name")
    }
  }

  override def name: String = s"InstagramProfileTask(userId=$profileId)"

}

