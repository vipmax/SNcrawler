package org.itmo.escience.core.crawlers

import akka.actor.{Actor, ActorSystem, Props}
import org.apache.log4j.Logger
import org.escience.core.osn.vkontakte.tasks.VkProfileTask
import org.itmo.escience.core.actors.SimpleWorkerActor
import org.itmo.escience.core.balancers.{Init, SimpleBalancer}
import org.itmo.escience.core.osn.vkontakte.tasks.{VkFollowersExtendedTask, VkSearchPostsTask, VkSearchPostsTaskResponse}
import org.itmo.escience.dao.{MongoSaverInfo, MongoSaverInfo2}
import org.itmo.escience.util.Util
import org.joda.time.DateTime

/**
  * Created by max on 15.12.16.
  */
object SlootCrawler {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("SlootCrawler")
    actorSystem.actorOf(Props[SlootCrawlerActor])  ! "start"
  }

  class SlootCrawlerActor extends Actor  {
    val logger = Logger.getLogger(this.getClass)
    val actorSystem = context.system
    val balancer = actorSystem.actorOf(Props[SimpleBalancer])

    0 to 10 foreach { _ =>
      val worker = actorSystem.actorOf(SimpleWorkerActor.props())
      worker.tell(Init(), balancer)
    }

    implicit val appname = "SlootCrawlerApp"
    implicit val ec = context.dispatcher

    private val mongoHost = Util.conf.getString("MongoHost")

    private val postsSaver = MongoSaverInfo(endpoint = mongoHost, db = "SlootCrawler", collection = "posts")
    private val profilesSaver = MongoSaverInfo(endpoint = mongoHost, db = "SlootCrawler", collection = "profiles")
    private val subscribersSaver = MongoSaverInfo2(endpoint = mongoHost, db = "SlootCrawler", collection = "subscribers", collection2 = "profiles")
    private val commentsSaver = MongoSaverInfo(endpoint = mongoHost, db = "SlootCrawler", collection = "comments")
    private val repostsSaver = MongoSaverInfo(endpoint = mongoHost, db = "SlootCrawler", collection = "reposts")
    private val likesSaver = MongoSaverInfo(endpoint = mongoHost, db = "SlootCrawler", collection = "likes")


    override def receive: Receive = {
      case "start" =>

        val keywords = List("антибиотик", "лихорадка", "инфекция мочевых путей", "грипп", "кашель", "аптека", "рецепт",
          "ципрофлоксацин", "котримоксазол", "амоксициллин", "доксициклин", "азитромицин")
//          .take(1)

        val fromTime = new DateTime(2016,1,1,0,0)
        val untilTime = new DateTime(2017,1,1,0,0)

        keywords.foreach{ keyword =>
          balancer ! VkSearchPostsTask(keyword,fromTime.getMillis / 1000, untilTime.getMillis / 1000, self, postsSaver)
        }

      case VkSearchPostsTaskResponse(params, data) =>
        logger.debug(s"Got VkSearchPostsTaskResponse(params=$params, data.length=${data.length})")
        val keyword = params("q")
        val startTime = params("start_time").toLong
        val endTime = params("end_time").toLong
        val halfTime = (startTime + endTime) / 2

        val stime = new DateTime(startTime * 1000).toString("yyyy-MM-dd HH:mm")
        val etime = new DateTime(endTime * 1000).toString("yyyy-MM-dd HH:mm")
        logger.debug(s"start_time = ${stime} end_time = ${etime} data.length=${data.length})")

        if (data.length == 200) {
          logger.debug("Creating more tasks")
          Thread.sleep(3000)
          balancer ! VkSearchPostsTask(keyword, startTime, halfTime, self, postsSaver)
          balancer ! VkSearchPostsTask(keyword, halfTime, endTime, self, postsSaver)
        } else {
          logger.debug(s"Task end! params = $params")
        }


        val ids = data.map(_.getString("from_id")).toList
        balancer ! VkProfileTask(ids, profilesSaver)

        ids.foreach { id => balancer ! VkFollowersExtendedTask(id, subscribersSaver) }

    }

  }
}
