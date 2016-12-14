package org.itmo.escience.core.osn.vkontakte.tasks

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.SaverInfo

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class VkLikesTask(itemType: String = "post",
                       ownerId: String,
                       itemId: String,
                       var count: Int = 1000,
                       saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def appname: String = app

  override def run(network: AnyRef): Unit = {
    var end = false
    var offset = 0

    while(!end) {
      val res = Http("https://api.vk.com/method/likes.getList?")
        .param("type", itemType.toString)
        .param("owner_id", ownerId.toString)
        .param("item_id", itemId.toString)
        .param("count", "1000")
        .param("offset", offset.toString)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
        .execute().body

      val likerIds = JSON.parse(res).asInstanceOf[BasicDBObject]
        .get("response").asInstanceOf[BasicDBObject]
        .get("items").asInstanceOf[BasicDBList].toArray()
        .map(e => e.asInstanceOf[Int])
        .map{likerId => new BasicDBObject()
          .append("key", s"${itemType}_${itemId}_${ownerId}_${likerId}")
          .append("itemType", itemType)
          .append("itemId", itemId.toLong)
          .append("ownerId", ownerId.toLong)
          .append("likerId", likerId.toLong)
        }

      offset += likerIds.length
      count -= likerIds.length

      if (likerIds.length < 1000 || count <= 0) end = true

      Option(saver) match {
        case Some(s) => likerIds.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }
    }
  }

  override def name: String = s"VkLikeTask(item=$ownerId)"

}

