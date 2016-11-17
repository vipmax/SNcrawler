package org.itmo.escience.core.osn.vkontakte.tasks

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject, DBObject}
import org.itmo.escience.core.osn.common.VkontakteTask

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
class VkUserPostsTask(userId:String)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkUserPostsTask(userId=$userId)"

  override def appname: String = app

  var result: Array[BasicDBObject] = null

  override def run(network: AnyRef): Unit = {

    var end = false
    var offset = 0

    while(!end) {
      val json = Http("https://api.vk.com/method/wall.get")
        .param("owner_id", userId.toString)
        .param("count", "100")
        .param("offset", offset.toString)
        .param("v", offset.toString)
        .timeout(60 * 1000 * 10, 60 * 1000 * 10)
        .execute().body

      val parsed = try {
        JSON.parse(json).asInstanceOf[DBObject]
          .get("response").asInstanceOf[BasicDBList].toArray()
      } catch {case e: Exception => Array[BasicDBList]()}

      val posts: Array[BasicDBObject] = parsed.filter(_.isInstanceOf[BasicDBObject]).map(_.asInstanceOf[BasicDBObject])

      if (posts.length < 100) end = true
      offset += posts.length

      posts.foreach{println}
      result = posts
    }
  }


  override def get() = result

}
