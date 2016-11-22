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
class VkUserPostsTask(userId:String, saver: Saver = null)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkPostsTask(userId=$userId)"

  override def appname: String = app

  var result: BasicDBObject = null

  override def run(network: AnyRef): Unit = {

    var end = false
    var offset = 0
    val maxPostsCount = 100

    while(!end) {
      val json = Http("https://api.vk.com/method/wall.get")
        .param("owner_id", userId.toString)
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
      } catch {case e: Exception => Array[BasicDBObject]()}

      if (posts.length < maxPostsCount) end = true
      offset += posts.length

//      posts.foreach{println}

      Option(saver) match {
        case Some(s) => posts.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }
    }
  }


  override def get() = result

}


object TestUserPosts {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[VkBalancer])

    1 until 10 foreach { i=>
      actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

//        balancer ! new VkUserPostsTask("1", MongoSaver("192.168.13.133","test_db","test_posts_collection"))
    balancer ! new VkUserPostsTask("1", KafkaUniqueSaver("192.168.13.133:9092","localhost", "test_posts"))

  }
}