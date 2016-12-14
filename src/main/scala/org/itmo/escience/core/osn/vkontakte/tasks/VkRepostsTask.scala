package org.itmo.escience.core.osn.vkontakte.tasks

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao.SaverInfo

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class VkRepostsTask(ownerId: String,
                         postId: String,
                         var count: Int = 1000,
                         saverInfo: SaverInfo)(implicit app: String) extends VkontakteTask {

  override def appname: String = app

  override def run(network: AnyRef): Unit = {
    var end = false
    var offset = 0

    while(!end) {
      val request = Http("https://api.vk.com/method/wall.getReposts?")
        .param("owner_id", ownerId.toString)
        .param("post_id", postId.toString)
        .param("count", "1000")
        .param("offset", offset.toString)
        .param("v", "5.8")
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)

      val reposts = JSON.parse(request.execute().body).asInstanceOf[BasicDBObject]
        .get("response").asInstanceOf[BasicDBObject]
        .get("items").asInstanceOf[BasicDBList].toArray()
        .map{ case b:BasicDBObject =>
          b.append("key", s"${b.getString("from_id")}_${b.getString("id")}")
        }


      offset += reposts.length
      count -= reposts.length

      if (reposts.length < 1000 || count <= 0) end = true

      Option(saver) match {
        case Some(s) => reposts.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }
    }
  }

  override def name: String = s"VkLikeTask(item=$ownerId)"

}

