package org.itmo.escience.core.crawlers

import akka.actor.{Actor, ActorSystem, Props}
import com.mongodb.{BasicDBObject, MongoClient}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.twitter.{TwitterParrallelTypedWorkerActor, TwitterSequentialTypedWorkerActor}
import org.itmo.escience.core.balancers.{Init, TwitterBalancer}
import org.itmo.escience.core.osn.common.Filters.TimeFilter
import org.itmo.escience.core.osn.twitter.tasks.{TwitterFollowersTask, TwitterFollowersTaskResponse, TwitterPostsTask, TwitterPostsTaskResponse}
import org.itmo.escience.dao.{FileSaverInfo, MongoSaverInfo}
import org.itmo.escience.util.Util
import org.joda.time.DateTime

/**
  * Created by max on 06.12.16.
  */
object TrumpGroupCrawler {
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
        saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "Trump", collection = "SeedPosts")
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

    val iterable = new MongoClient("192.168.13.133").getDatabase("Trump").getCollection("users").find()
    iterable.foreach{ d =>
      val id = d.getLong("follower")

      balancer ! TwitterPostsTask(
        profileId = id,
        timeFilter = TimeFilter(new DateTime().minusMonths(6), DateTime.now()),
        saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "Trump", collection = "Followers_Posts")
//        saverInfo = FileSaverInfo("/mnt/share/Petrov/mongodata/Followers_Posts.json")
      )
      Thread.sleep(100)
    }
  }

}

object TrumpCrawler {

  def main(args: Array[String]) {
    val actorSystem = ActorSystem("TrumpCrawler")
    actorSystem.actorOf(Props[TrumpCrawlerActor])  ! "start"
  }

  class TrumpCrawlerActor extends Actor  {
    val logger = Logger.getLogger(this.getClass)
    val actorSystem = context.system
    val balancer = actorSystem.actorOf(Props[TwitterBalancer])

    Util.getTwitterAccounts() foreach { account =>
      val workerProps = TwitterParrallelTypedWorkerActor.props(account)
      val worker = actorSystem.actorOf(workerProps)
      worker.tell(Init(), balancer)
    }

    implicit val appname = "TrumpCrawlerApp"
    implicit val ec = context.dispatcher

    private val mongoHost = Util.conf.getString("MongoHost")

    val mongoDatabase = new MongoClient().getDatabase("TrumpCrawler")
    val stateCollection = mongoDatabase.getCollection("appstate", classOf[BasicDBObject])

//    val saver1 = MongoSaverInfo(endpoint = mongoHost, db = "TrumpCrawler", collection = "SeedUsers")
//    val saver2 = MongoSaverInfo(endpoint = mongoHost, db = "TrumpCrawler", collection = "SeedPosts")
//    val saver3 = MongoSaverInfo(endpoint = mongoHost, db = "TrumpCrawler", collection = "SeedUsersPosts")

    val saver1 = FileSaverInfo("/mnt/share/Petrov/mongodata/Trump/SeedUsers.json")
    val saver2 = FileSaverInfo("/mnt/share/Petrov/mongodata/Trump/SeedPosts.json")
    val saver3 = FileSaverInfo("/mnt/share/Petrov/mongodata/Trump/SeedUsersPosts.json")


    override def receive: Receive = {
      case "start" =>
        val groups = List(807095, 2467791, 5988062)

        groups.foreach { g =>
          balancer ! TwitterFollowersTask(
            profileId = g,
            count = 5000000,
            responseActor = self,
            saverInfo = saver1
          )
        }

        groups.foreach { g=>
          balancer ! TwitterPostsTask(
            profileId = g,
            timeFilter = TimeFilter(new DateTime().minusMonths(6), DateTime.now()),
            responseActor = self,
            saverInfo = saver2
          )
        }

      case TwitterFollowersTaskResponse(profileId, followers) =>
        logger.debug(s"Got TwitterFollowersTaskResponse for $profileId followers count = ${followers.length}")

        followers.foreach { follower=>
          balancer ! TwitterPostsTask(
            profileId = follower,
            timeFilter = TimeFilter(new DateTime().minusMonths(6), DateTime.now()),
            responseActor = self,
            saverInfo = saver3
          )
        }

      case  TwitterPostsTaskResponse(profileId, posts) =>
        logger.debug(s"Got TwitterPostsTaskResponse for $profileId posts count = ${posts.length}")
//        MongoUtil.update(stateCollection, "last_user_id", new BasicDBObject("user_id", profileId))
    }

  }
}
