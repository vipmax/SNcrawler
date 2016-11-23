package org.escience.core.osn.vkontakte.tasks

import akka.actor.{ActorSystem, Props}
import com.mongodb.{BasicDBList, BasicDBObject, DBObject}
import com.mongodb.util.JSON
import org.itmo.escience.core.actors.VkSimpleWorkerActor
import org.itmo.escience.core.balancers.{Init, VkBalancer}
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.{KafkaUniqueSaver, MongoSaver, Saver, SaverInfo}

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class VkProfileTask(profileIds: List[String], saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkProfileTask(userId=$profileIds)"
  override def appname: String = app

  var result: BasicDBObject = null

  val group_fields = "city, country, place, description, wiki_page, members_count, " +
    "counters, start_date, finish_date, can_post, can_see_all_posts, activity," +
    " status, contacts, links, fixed_post, verified, site,ban_info"

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

  override def run(network: AnyRef) {
    val users = profileIds.map(_.toLong).filter(_ > 0).mkString(",")
    val groups = profileIds.map(_.toLong).filter(_ < 0).map(-_).mkString(",")

    if(users.nonEmpty) {
      val json = Http(s"https://api.vk.com/method/users.get")
        .param("user_ids", users)
        .param("fields", userFields)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
        .execute().body

      logger.debug(json)
      val profiles = parse(json)
      logger.debug(profiles)
      save(profiles)
    }

    if(groups.nonEmpty) {
      val json = Http(s"https://api.vk.com/method/groups.getById")
        .param("group_ids", users)
        .param("fields", group_fields)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
        .execute().body

      logger.debug(json)
      val profiles = parse(json).map { case bdo: BasicDBObject =>
        bdo.append("key", s"-${bdo.getInt("id")}")
      }
      logger.debug(profiles)
      save(profiles)
    }
  }

  def parse(json: String): Array[BasicDBObject] = {
    try { JSON.parse(json)
            .asInstanceOf[DBObject]
            .get("response").asInstanceOf[BasicDBList]
            .toArray().map { case bdo: BasicDBObject =>
              bdo.append("key", s"${bdo.getInt("id")}")
            }
    } catch {case e: Exception =>
      logger.error(json)
      Array[BasicDBObject]()
    }
  }

  def save(profiles: Array[BasicDBObject]) {
    Option(saver) match {
      case Some(s) => profiles.foreach(s.save)
      case None => logger.debug(s"No saver for task $name")
    }
  }
}

