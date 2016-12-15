package org.itmo.escience.core.osn.vkontakte.tasks

import akka.actor.ActorRef
import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.SaverInfo
import org.joda.time.DateTime

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */

case class VkSearchPostsTaskResponse(params: Map[String, String], data: Array[BasicDBObject])

case class VkSearchPostsTask(query: String,
                             startTime: Long = DateTime.now().minusDays(1).getMillis / 1000,
                             endTime: Long = DateTime.now().getMillis / 1000,
                             responseActor: ActorRef = null,
                             saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkSearchPostsTask(query=$query, startTime=$startTime, endTime=$endTime)"

  override def appname: String = app

  val request = Http("https://api.vk.com/method/newsfeed.search")
    .param("q", query.toString)
    .param("count", "200")
    .param("start_time", startTime.toString)
    .param("end_time", endTime.toString)
    .param("v", "5.8")

  override def run(network: AnyRef) {

    val json = request
      .timeout(60 * 1000 * 10, 60 * 1000 * 10)
      .execute().body

    val posts = try {
      JSON.parse(json).asInstanceOf[BasicDBObject]
        .get("response").asInstanceOf[BasicDBObject]
        .get("items").asInstanceOf[BasicDBList].toArray
        .map{ case b:BasicDBObject =>
          b.append("key", s"${b.getString("from_id")}_${b.getString("id")}")
            .append("search_query_params", request.params.mkString(", "))
        }
    } catch { case e: Exception =>
      logger.error(e)
      logger.error(json)
      Array[BasicDBObject]()
    }

    Option(saver) match {
      case Some(s) => posts.foreach(s.save)
      case None => logger.debug(s"No saver for task $name")
    }
    Option(responseActor) match {
      case Some(actor) => actor ! VkSearchPostsTaskResponse(request.params.toMap, posts)
      case None => logger.debug(s"No response Actor for task $name")
    }
  }
}

case class VkSearchPostsExtendedTask(params: Map[String, String],
                                     responseActor: ActorRef = null,
                                     saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkSearchPostsExtendedTask(params=$params)"

  override def appname: String = app

  val request = Http("https://api.vk.com/method/newsfeed.search")
    .params(params)
    .param("v", "5.8")
    .timeout(60 * 1000 * 10, 60 * 1000 * 10)

  override def run(network: AnyRef) {

    val json = request.execute().body

    val posts = try {
      JSON.parse(json).asInstanceOf[BasicDBObject]
        .get("response").asInstanceOf[BasicDBObject]
        .get("items").asInstanceOf[BasicDBList].toArray
        .map{ case b:BasicDBObject => b.append("key", s"${b.getString("from_id")}_${b.getString("id")}")
          .append("search_query_params", request.params.toMap)
        }
    } catch { case e: Exception =>
      logger.error(e)
      logger.error(json)
      Array[BasicDBObject]()
    }

    Option(saver) match {
      case Some(s) => posts.foreach(s.save)
      case None => logger.debug(s"No saver for task $name")
    }
    Option(responseActor) match {
      case Some(actor) => actor ! VkSearchPostsTaskResponse(request.params.toMap, posts)
      case None => logger.debug(s"No response Actor for task $name")
    }
  }
}

