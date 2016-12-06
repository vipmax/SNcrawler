package org.itmo.escience.core.crawlers

import akka.actor.{ActorSystem, Props}
import com.mongodb.MongoClient
import org.itmo.escience.core.actors.TwitterSequentialTypedWorkerActor
import org.itmo.escience.core.balancers.{Init, TwitterBalancer}
import org.itmo.escience.core.osn.common.Filters.TimeFilter
import org.itmo.escience.core.osn.twitter.tasks.{TwitterFollowersTask, TwitterPostsTask}
import org.itmo.escience.dao.MongoSaverInfo
import org.itmo.escience.util.Util
import org.joda.time.DateTime

/**
  * Created by max on 06.12.16.
  */
object TrumpCrawler {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(1)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "TrumpCrawlerApp"


    val groups = List("@nytimes","@washingtonpost", "@TheEconomist")
//    val groups = List("@nytimes")

    groups.foreach { g =>
      balancer ! TwitterPostsTask(
        profileId = g,
        timeFilter = TimeFilter(new DateTime().minusMonths(6), DateTime.now()),
        saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "Trump", collection = "Posts")
      )
    }
  }
}

object TrumpFollowersCrawler {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts().take(100)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "TrumpCrawlerApp"


    val groups = List("@nytimes","@washingtonpost", "@TheEconomist")
//    val groups = List("@nytimes")

    groups.foreach { g =>
      balancer ! TwitterFollowersTask(
        profileId = g,
        saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "Trump", collection = "users")
      )
    }
  }
}

object TrumpFollowersPostsCrawler {
  import collection.JavaConversions._

  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TwitterBalancer")
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    val accounts = Util.getTwitterAccounts()

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSequentialTypedWorkerActor.props(account)).tell(Init(), balancer)
    }

    implicit val appname = "TrumpCrawlerApp"

    new MongoClient("192.168.13.133").getDatabase("Trump").getCollection("users").find().foreach{d =>
      val id = d.getLong("follower")
//      println(id)

      balancer ! TwitterPostsTask(
        profileId = id,
        timeFilter = TimeFilter(new DateTime().minusMonths(6), DateTime.now()),
        saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "Trump", collection = "Followers_Posts")
      )
      Thread.sleep(100)
    }
  }
}
