package org.itmo.escience.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.SimpleWorkerActor.SimpleWorkerTaskRequest
import org.itmo.escience.core.balancers.Init
import org.itmo.escience.core.osn.common._
import org.itmo.escience.dao._
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterFactory}
/**
  * Created by vipmax on 14.11.16.
  */
object SimpleWorkerActor {
  def props(account: Account = null) = Props(new SimpleWorkerActor(account))

  case class SimpleWorkerTaskRequest(task: Task)
}

class SimpleWorkerActor(account: Account = null) extends Actor {
  val logger = Logger.getLogger(this.getClass)
  val network = buildNetwork(account)
  var balancer: ActorRef = _

  override def receive: Receive = {
    case task: Task =>
      logger.debug(s"task = $task ${task.getClass}")

      task.logger = Logger.getLogger(s"${task.appname} ${task.name}")

      task.saverInfo match {
        case MongoSaverInfo(endpoint:String, db:String, collection:String) =>
          logger.debug(s"Found saver {mongo $endpoint, $db, $collection}")
          task.saver = MongoSaver(endpoint, db, collection)

        case MongoSaverInfo2(endpoint:String, db:String, collection:String, collection2:String) =>
          logger.debug(s"Found saver {mongo $endpoint, $db, $collection}")
          task.saver = MongoSaver(endpoint, db, collection)
          task.saver2 = MongoSaver(endpoint, db, collection2)

        case KafkaSaverInfo(endpoint:String, topic:String) =>
          logger.debug(s"Found saver {kafka $endpoint, $topic}")
          task.saver = KafkaSaver(endpoint, topic)

        case KafkaUniqueSaverInfo(kafkaEndpoint:String,redisEndpoint:String, topic:String) =>
          logger.debug(s"Found saver {kafka unique $kafkaEndpoint,$redisEndpoint, $topic}")
          task.saver = KafkaUniqueSaver(kafkaEndpoint, redisEndpoint, topic)

        case _ => logger.debug("Unknown saver")
      }

      try {
        task.run(network)
      } catch {
        case e: Exception => logger.error(e)
      }

      balancer ! SimpleWorkerTaskRequest(task)


    case Init() =>
      logger.debug(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! SimpleWorkerTaskRequest(null)

    case _ =>
      throw new RuntimeException("World is burning!!")
  }


  private def buildNetwork(account: Account) = {
    account match {
      case a: TwitterAccount => buildTwitter(a)
      case a: VkontakteAccount => a
      case a: InstagramAccount => a
      case _ => null
    }
  }

  private def buildTwitter(twitterAccount: TwitterAccount): Twitter = {
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setJSONStoreEnabled(true)
      .setOAuthConsumerKey(twitterAccount.key)
      .setOAuthConsumerSecret(twitterAccount.secret)
      .setOAuthAccessToken(twitterAccount.token)
      .setOAuthAccessTokenSecret(twitterAccount.tokenSecret)
    val twitter = new TwitterFactory(cb.build()).getInstance()
    logger.debug(twitter)
    twitter
  }

}



