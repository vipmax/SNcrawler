package org.itmo.escience.core.osn.vkontakte.tasks

import akka.actor.{ActorSystem, Props}
import org.escience.core.osn.vkontakte.tasks.VkProfileTask
import org.itmo.escience.core.actors.VkSimpleWorkerActor
import org.itmo.escience.core.balancers.{Init, VkBalancer}
import org.itmo.escience.dao.{KafkaUniqueSaver, MongoSaver}

/**
  * Created by vipmax on 22.11.16.
  */

object TestGroupFollowers {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[VkBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    //    balancer ! new VkGroupFollowersTask("1", MongoSaver("192.168.13.133","test_db","test_group_relations_collection"))

    balancer ! new VkGroupFollowersExtendedTask(
      userId = "1",
      relationsSaver = MongoSaver("192.168.13.133","test_db", "test_group_relations_collection"),
      usersSaver = MongoSaver("192.168.13.133","test_db", "test_users_collection")
    )
  }
}


object TestProfile {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[VkBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    val vkGroupProfileTask = new VkProfileTask(
      profileIds = List("1", "2", "-1", "-2"),
      saver = MongoSaver("192.168.13.133", "test_db", "test_collection")
    )
    balancer ! vkGroupProfileTask

  }
}

object TestUserFollowers {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[VkBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    //    balancer ! new VkFriendsTask("1", MongoSaver("192.168.13.133","test_db","test_relations_collection"))

    balancer ! new VkGroupFollowersExtendedTask(
      userId = "1",
      relationsSaver = MongoSaver("192.168.13.133","test_db", "test_relations_collection"),
      usersSaver = MongoSaver("192.168.13.133","test_db", "test_users_collection")
    )
  }
}


object TestUserPosts {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[VkBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    //        balancer ! new VkUserPostsTask("1", MongoSaver("192.168.13.133","test_db","test_posts_collection"))
    balancer ! new VkPostsTask("1", KafkaUniqueSaver("192.168.13.133:9092","localhost", "test_posts"))

  }
}

