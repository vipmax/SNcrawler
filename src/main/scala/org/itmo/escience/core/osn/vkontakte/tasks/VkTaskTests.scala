package org.itmo.escience.core.osn.vkontakte.tasks

import akka.actor.{ActorSystem, Props}
import com.mongodb.{BasicDBObject, MongoClient}
import com.typesafe.config.ConfigFactory
import org.escience.core.osn.vkontakte.tasks.VkProfileTask
import org.itmo.escience.core.actors.VkSimpleWorkerActor
import org.itmo.escience.core.balancers.{Init, TwitterBalancer, VkBalancer}
import org.itmo.escience.dao._
import org.joda.time.DateTime

/**
  * Created by vipmax on 22.11.16.
  */

object TestProfile {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    val vkGroupProfileTask = new VkProfileTask(
      profileIds = List("1", "2", "-1", "-2"),
      saverInfo = KafkaSaverInfo(endpoint = "192.168.13.133:9092", topic = "test")
    )
    balancer ! vkGroupProfileTask
  }
}


object TestRemoteProfile {
  def main(args: Array[String]) {
    val ip = "127.0.0.1"
    val akkaSystemName = "App"

    val config: String = s"""
      akka {
          actor {
            provider = "akka.remote.RemoteActorRefProvider"
          }
          remote {
            log-remote-lifecycle-events = off
            netty.tcp {
              hostname = "$ip"
              port = 0
            }
          }
          serializers {
            java = "akka.serialization.JavaSerializer"
          }
      }
    """
    val actorSystem = ActorSystem(akkaSystemName, ConfigFactory.parseString(config))
    val balancer = actorSystem.actorSelection("akka.tcp://VkBalancer@127.0.0.1:2551/user/balancer")

    implicit val appname = "testApp"

    val vkGroupProfileTask = new VkProfileTask(
      profileIds = List("1", "2", "-1", "-2"),
      saverInfo = KafkaSaverInfo(endpoint = "192.168.13.133:9092", topic = "test")
    )

    balancer ! vkGroupProfileTask
  }
}

object TestFollowers {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    balancer ! new VkFollowersTask(
      profileId = "-1",
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_relations")
    )
  }
}

object TestFollowersExtended {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    balancer ! new VkFollowersExtendedTask(
      profileId = "-1",
      saverInfo = MongoSaverInfo2(endpoint = "192.168.13.133", db = "test_db", collection = "test_relations", collection2 = "test_users")
    )
  }
}

object TestPosts {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    balancer ! new VkPostsTask(
      ownerId = "-1",
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_posts")
    )
  }
}

object TestSearchPosts {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "testApp"

    balancer ! new VkSearchPostsTask(
      query = "spb",
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_posts_spb")
    )
  }
}


object PrichislenkoCrawler {
  def main(args: Array[String]) {

    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[VkBalancer])

    actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)

    implicit val appname = "PrichislenkoApp"

    val ids = Array(
      "-41240468",
      "-74058720",
      "-50305445",
      "-81526971",
      "-47214165",
      "-465",
      "-36286006",
      "-55821382",
      "-30525261",
      "-23611958",
      "-38119975",
      "-41538339",
      "-86218441"
    )

    ids.foreach{ id =>
      balancer ! new VkPostsTask(
        ownerId = id,
        saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "Prichislenko", collection = "posts")
      )
    }
  }
}
