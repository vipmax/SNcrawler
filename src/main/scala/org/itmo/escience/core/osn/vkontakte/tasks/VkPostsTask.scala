package org.itmo.escience.core.osn.vkontakte.tasks

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.SaverInfo

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class VkPostsTask(ownerId:String, saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  var result: BasicDBObject = null

  override def appname: String = app

  override def run(network: AnyRef): Unit = {

    var end = false
    var offset = 0
    val maxPostsCount = 100

    while(!end) {
      val json = Http("https://api.vk.com/method/wall.get")
        .param("owner_id", ownerId.toString)
        .param("count", maxPostsCount.toString)
        .param("offset", offset.toString)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
        .execute().body

      val posts: Array[BasicDBObject] = try {
        JSON.parse(json).asInstanceOf[BasicDBObject]
          .get("response").asInstanceOf[BasicDBObject]
          .get("items").asInstanceOf[BasicDBList].toArray
          .map{ case b:BasicDBObject =>
            b.append("key", s"${b.getString("from_id")}_${b.getString("id")}")
          }
      } catch {case e: Exception =>
        logger.error(json)
        Array[BasicDBObject]()
      }

      if (posts.length < maxPostsCount) end = true
      offset += posts.length

      Option(saver) match {
        case Some(s) => posts.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }
    }
  }

  override def name: String = s"VkPostsTask(userId=$ownerId)"



}

