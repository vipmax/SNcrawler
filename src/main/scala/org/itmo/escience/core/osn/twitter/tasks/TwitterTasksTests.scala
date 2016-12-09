package org.itmo.escience.core.osn.twitter.tasks

import akka.actor.{ActorSystem, Props}
import org.itmo.escience.core.actors.twitter.TwitterSequentialTypedWorkerActor
import org.itmo.escience.core.balancers.{Init, TwitterBalancer}
import org.itmo.escience.dao.MongoSaverInfo
import org.itmo.escience.util.Util
import twitter4j.Query

/**
  * Created by vipmax on 29.11.16.
  */
object TwitterPostsTaskTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterPostsTask(
//      profileId = "@djvipmax_",
      profileId = "@Rogozin",
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_twitter")
    )
  }
}


object TwitterFollowersTaskTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterFollowersTask(
//      profileId = "@djvipmax_",
      profileId = "@Rogozin",
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_twitter_followers")
    )
  }
}

object TwitterProfileTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

    balancer ! TwitterProfileTask(
      //      profileId = "@djvipmax_",
      profileId = "@Rogozin",
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_user_profiles")
    )
  }
}

object TwitterSearchPostsTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

    val query = new Query()
    query.setQuery("warriors")

    balancer ! TwitterSearchPostsTask(
      query = query,
      count = 140,
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "test_warriors_tweets")
    )
  }
}
