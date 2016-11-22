package org.itmo.escience.core.osn.vkontakte.tasks

import akka.actor.{ActorSystem, Props}
import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject, DBObject}
import org.itmo.escience.core.actors.VkSimpleWorkerActor
import org.itmo.escience.core.balancers.{Init, VkBalancer}
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.{KafkaUniqueSaver, MongoSaver, Saver}

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
class VkUserFollowersTask(userId:String, saver: Saver = null)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkUserFollowersTask(userId=$userId)"

  override def appname: String = app

  var result: Array[BasicDBObject] = null

  override def run(network: AnyRef): Unit = {

    var end = false
    var offset = 0
    val maxCount = 1000
    while(!end) {
      val res = Http("https://api.vk.com/method/friends.get")
        .param("user_id", userId.toString)
        .param("offset", offset.toString)
        .param("count", maxCount.toString)
        .param("v", "5.8")
        .execute().body
      logger.debug(res)

      val friends = try {
        JSON.parse(res).asInstanceOf[BasicDBObject]
       .get("response").asInstanceOf[BasicDBObject]
       .get("items").asInstanceOf[BasicDBList].toArray.map(_.asInstanceOf[Int])
       .map{f => new BasicDBObject()
           .append("key", s"${userId}_${f}")
           .append("user", userId)
           .append("friend", f)
       }
      } catch {case e: Exception => Array[BasicDBObject]()}


      if (friends.length < maxCount) end = true
      offset += friends.length

      Option(saver) match {
        case Some(s) => friends.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

      result = friends
    }
  }

  override def get() = result
}

class VkUserFollowersExtendedTask(userId:String, relationsSaver: Saver = null, usersSaver: Saver = null)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkUserFollowersExtendedTask(userId=$userId)"

  override def appname: String = app

  var result: Array[BasicDBObject] = null

  val fields =  "photo_id, verified, sex, bdate, city, country, home_town, " +
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

  override def run(network: AnyRef): Unit = {

    var end = false
    var offset = 0
    val maxCount = 1000
    while(!end) {
      val res = Http("https://api.vk.com/method/friends.get")
        .param("user_id", userId.toString)
        .param("offset", offset.toString)
        .param("count", maxCount.toString)
        .param("v", "5.8")
        .param("fields", fields)
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
        .execute().body

      val friends = try {
        JSON.parse(res).asInstanceOf[BasicDBObject]
          .get("response").asInstanceOf[BasicDBObject]
          .get("items").asInstanceOf[BasicDBList].toArray
          .map{ case bdo:BasicDBObject =>
            bdo.append("key", s"${userId}_${bdo.getInt("id")}")
          }

      } catch {case e: Exception => Array[BasicDBObject]()}


      if (friends.length < maxCount) end = true
      offset += friends.length

      Option(relationsSaver) match {
        case Some(s) =>
          friends.map{ case bdo => new BasicDBObject()
              .append("user", userId)
              .append("key", bdo.getString("key"))
              .append("friend", bdo.getString("id"))
          }.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

      Option(usersSaver) match {
        case Some(s) => friends.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }

      result = friends
    }
  }

  override def get() = result
}

