package org.itmo.escience.core.osn.twitter.tasks

import akka.actor.{ActorSystem, Props}
import org.itmo.escience.core.actors.{TwitterSimpleWorkerActor, VkSimpleWorkerActor}
import org.itmo.escience.core.balancers.{Init, TwitterBalancer}
import org.itmo.escience.core.osn.vkontakte.tasks.VkSearchPostsTask
import org.itmo.escience.dao.MongoSaverInfo
import org.itmo.escience.util.Util

/**
  * Created by vipmax on 29.11.16.
  */
object TwitterPostsTaskTests {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSimpleWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterPostsTask(
//      profileId = "@djvipmax_",
      profileId = "@Rogozin",
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_twitter")
    )
  }
}


object TwitterFollowersTaskTests {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSimpleWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterFollowersTask(
//      profileId = "@djvipmax_",
      profileId = "@Rogozin",
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_twitter_followers")
    )
  }
}
