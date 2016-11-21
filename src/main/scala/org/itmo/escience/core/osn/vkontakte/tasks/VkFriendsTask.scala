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
class VkFriendsTask(userId:String, saver: Saver = null)(implicit app: String) extends VkontakteTask {

  override def name: String = s"VkFriendsTask(userId=$userId)"

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

object testFriends {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[VkBalancer])

    1 until 10 foreach { i=>
      actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

//    balancer ! new VkFriendsTask("1", MongoSaver("192.168.13.133","test_db","test_relations_collection"))
    balancer ! new VkFriendsTask("1", KafkaUniqueSaver("192.168.13.133:9092","localhost", "test_relations_collection"))


  }
}