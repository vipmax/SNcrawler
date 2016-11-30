package org.itmo.escience.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.TwitterSimpleWorkerActor.TwitterSimpleWorkerTaskRequest
import org.itmo.escience.core.balancers.Init
import org.itmo.escience.core.osn.common.{TwitterAccount, TwitterTask}
import org.itmo.escience.dao.{KafkaUniqueSaver, KafkaUniqueSaverInfo, _}
import twitter4j.{Twitter, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder

/**
  * Created by vipmax on 29.11.16.
  */
object TwitterSimpleWorkerActor {
  def props(twitterAccount: TwitterAccount) = Props(new TwitterSimpleWorkerActor(twitterAccount))

  case class TwitterSimpleWorkerTaskRequest(task: TwitterTask)
}

class TwitterSimpleWorkerActor(twitterAccount: TwitterAccount) extends Actor {
  val logger = Logger.getLogger(this.getClass)
  var balancer: ActorRef = _
  var twitter: Twitter = buildTwitter()


  override def receive: Receive = {
    case task: TwitterTask =>
      logger.debug(s"task = $task ${task.getClass}")

      inject(task)
      task.run(twitter)
      balancer ! TwitterSimpleWorkerTaskRequest(task)

    case Init() =>
      logger.debug(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! TwitterSimpleWorkerTaskRequest(null)

    case _ =>
      throw new RuntimeException("World is burning!!")
  }

  def inject(task: TwitterTask): Unit = {
    task.logger = Logger.getLogger(s"${task.appname} ${task.name}")

    task.saverInfo match {
      case MongoSaverInfo(endpoint: String, db: String, collection: String) =>
        logger.debug(s"Found saver {mongo $endpoint, $db, $collection}")
        task.saver = new MongoSaver(endpoint, db, collection)

      case MongoSaverInfo2(endpoint: String, db: String, collection: String, collection2: String) =>
        logger.debug(s"Found saver {mongo $endpoint, $db, $collection}")
        task.saver = new MongoSaver(endpoint, db, collection)
        task.saver2 = new MongoSaver(endpoint, db, collection2)

      case KafkaSaverInfo(endpoint: String, topic: String) =>
        logger.debug(s"Found saver {kafka $endpoint, $topic}")
        task.saver = new KafkaSaver(endpoint, topic)

      case KafkaUniqueSaverInfo(kafkaEndpoint: String, redisEndpoint: String, topic: String) =>
        logger.debug(s"Found saver {kafka unique $kafkaEndpoint,$redisEndpoint, $topic}")
        task.saver = new KafkaUniqueSaver(kafkaEndpoint, redisEndpoint, topic)

      case _ => logger.debug("Unknown saver")
    }
  }

  def buildTwitter(): Twitter = {
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


