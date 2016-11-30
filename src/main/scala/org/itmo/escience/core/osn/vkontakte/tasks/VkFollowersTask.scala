package org.itmo.escience.core.osn.vkontakte.tasks

import akka.actor.{ActorSystem, Props}
import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject, DBObject}
import org.itmo.escience.core.actors.VkSimpleWorkerActor
import org.itmo.escience.core.balancers.{Init, TwitterBalancer}
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.{KafkaUniqueSaver, MongoSaver, Saver, SaverInfo}

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */

case class VkFollowersTask(profileId:String, saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkFollowersTask(userId=$profileId)"
  override def appname: String = app

  var result: Array[BasicDBObject] = null

  val methodName = if (profileId.toLong > 0)  "friends.get" else "groups.getMembers"
  val paramProfileKey = if (profileId.toLong > 0)  "user_id" else "group_id"
  val paramProfileValue = if (profileId.toLong > 0)  profileId else profileId.substring(1)
  val parseKey = if (profileId.toLong > 0) "items" else "users"

  override def run(network: AnyRef) {
    var end = false
    var offset = 0
    val maxCount = 1000

    while(!end) {
      val res = Http(s"https://api.vk.com/method/$methodName")
        .param(paramProfileKey, paramProfileValue)
        .param("offset", offset.toString)
        .param("count", maxCount.toString)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
        .execute().body
      logger.debug(res)

      val followers = try {
        JSON.parse(res).asInstanceOf[BasicDBObject]
          .get("response").asInstanceOf[BasicDBObject]
          .get(parseKey).asInstanceOf[BasicDBList].toArray.map(_.asInstanceOf[Int])
          .map{f => new BasicDBObject()
            .append("key", s"${profileId}_$f")
            .append("profile", profileId)
            .append("follower", f)
          }
      } catch {case e: Exception =>
        logger.error(res)
        Array[BasicDBObject]()
      }

      if (followers.length < maxCount) end = true
      offset += followers.length

      Option(saver) match {
        case Some(s) => followers.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

      result = followers
    }
  }

}

case class VkFollowersExtendedTask(profileId:String, saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkUserFollowersExtendedTask(userId=$profileId)"

  override def appname: String = app

  val methodName = if (profileId.toLong > 0)  "friends.get" else "groups.getMembers"
  val paramProfileKey = if (profileId.toLong > 0)  "user_id" else "group_id"
  val paramProfileValue = if (profileId.toLong > 0)  profileId else profileId.substring(1)
  val parseKey = if (profileId.toLong > 0) "items" else "users"

  val userFields =  "photo_id, verified, sex, bdate, city, country, home_town, " +
    "has_photo, photo_50, photo_100, photo_200_orig, photo_200, " +
    "photo_400_orig, photo_max, photo_max_orig, online, lists, " +
    "domain, has_mobile, contacts, site, education, universities, " +
    "schools, status, last_seen, followers_count,  occupation, nickname, " +
    "relatives, relation, personal, connections, exports, wall_comments, " +
    "activities, interests, music, movies, tv, books, games, about, quotes, " +
    "can_post, can_see_all_posts, can_see_audio, can_write_private_message, " +
    "can_send_friend_request, is_favorite, is_hidden_from_feed, timezone, " +
    "screen_name, maiden_name, crop_photo, is_friend, friend_status, career," +
    " military, blacklisted, blacklisted_by_me"

  val groupFields =  "sex, bdate, city, country, photo_50, photo_100, photo_200_orig, " +
    "photo_200, photo_400_orig, photo_max, photo_max_orig, online, online_mobile," +
    " lists, domain, has_mobile, contacts, connections, site, education, universities," +
    " schools, can_post, can_see_all_posts, can_see_audio, can_write_private_message," +
    " status, last_seen, relation, relatives"

  val fields = if (profileId.toLong > 0) userFields else groupFields


  override def run(network: AnyRef): Unit = {

    var end = false
    var offset = 0
    val maxCount = 1000

    while(!end) {
      val res = Http(s"https://api.vk.com/method/$methodName")
        .param(paramProfileKey, paramProfileValue)
        .param("offset", offset.toString)
        .param("count", maxCount.toString)
        .param("v", "5.8")
        .param("fields", fields)
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
        .execute().body

      val friends = try {
        JSON.parse(res).asInstanceOf[BasicDBObject]
          .get("response").asInstanceOf[BasicDBObject]
          .get(parseKey).asInstanceOf[BasicDBList].toArray
      } catch {case e: Exception =>
        logger.error(res)
        Array[BasicDBObject]()
      }


      if (friends.length < maxCount) end = true
      offset += friends.length


      Option(saver) match {
        case Some(s) => friends.map { case bdo:BasicDBObject =>
          new BasicDBObject()
            .append("key", s"${profileId}_${bdo.getString("id")}")
            .append("profile", profileId)
            .append("follower", bdo.getString("id"))
        }.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }
      Option(saver2) match {
        case Some(s) =>
          friends.map{ case bdo:BasicDBObject =>
            bdo.append("key", bdo.getInt("id"))
          }.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

    }
  }
}


