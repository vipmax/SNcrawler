package org.itmo.escience.core.osn.instagram

import akka.actor.{ActorSystem, Props}
import org.itmo.escience.core.actors.SimpleWorkerActor
import org.itmo.escience.core.balancers.{Init, SimpleBalancer}
import org.itmo.escience.dao.MongoSaverInfo

import scalaj.http.Http

/**
  * Created by max on 02.12.16.
  */
object InstagramProfileTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[SimpleBalancer])

    actorSystem.actorOf(SimpleWorkerActor.props()).tell(Init(), balancer)

    implicit val appname = "testApp"

    balancer ! InstagramProfileTask(
      //      profileId = "boldasarini_make_up",
      profileId = 390955269L,
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "instagram_profiles_test")
    )
  }
}


object InstagramSearchTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[SimpleBalancer])

    actorSystem.actorOf(SimpleWorkerActor.props()).tell(Init(), balancer)

    implicit val appname = "testApp"

    balancer ! InstagramSearchPostsTask(
      query = "spb",
      count = 1000,
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "instagram_spb_test")
    )
  }
}

object InstagramPostsTest {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[SimpleBalancer])

    actorSystem.actorOf(SimpleWorkerActor.props()).tell(Init(), balancer)

    implicit val appname = "testApp"

    balancer ! InstagramPostsTask(
      username = "boldasarini_make_up",
      //      count = 1000,
      saverInfo = MongoSaverInfo(endpoint = "192.168.13.133", db = "test_db", collection = "instagram_posts_test")
    )
  }
}


object insttest {
  def main(args: Array[String]): Unit = {

    val username = "boldasarini_make_up"
    //    val response = Jsoup.connect(s"https://instagram.com/$username/?__a=1").ignoreContentType(true).execute().body()

    val response = Http(s"https://instagram.com/$username/?__a=1").header("User-Agent", "ScalaBOT 1.0").execute()
    println(response)

    /* wtf ??? */

  }
}