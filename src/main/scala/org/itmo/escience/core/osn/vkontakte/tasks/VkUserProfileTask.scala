package org.itmo.escience.core.osn.vkontakte.tasks

import com.mongodb.{BasicDBList, BasicDBObject, DBObject}
import com.mongodb.util.JSON
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.Saver
import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
class VkUserProfileTask(userId: String, saver: Saver = null)(implicit app: String) extends VkontakteTask {

  var result: BasicDBObject = null
  val userFields = "photo_id, verified, sex, bdate, city, country, home_town, " +
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


  override def name: String = s"VkUserProfileTask(userId=$userId)"
  override def appname: String = app
  override def get() = result

  override def run(network: AnyRef) {
    val json = Http("https://api.vk.com/method/users.get")
      .param("user_ids", userId)
      .param("fields", userFields)
      .param("v", "5.8")
      .execute().body

    result = JSON.parse(json)
      .asInstanceOf[DBObject]
      .get("response").asInstanceOf[BasicDBList]
      .toArray().map(_.asInstanceOf[BasicDBObject]).head

    logger.debug(result)

    /* needs for saver */
    result.append("key", result.getString("id"))

    Option(saver) match {
      case Some(s) => s.save(result)
      case None => logger.debug(s"No saver for task $name")
    }
  }
}
