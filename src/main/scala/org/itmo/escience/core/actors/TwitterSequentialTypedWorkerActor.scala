package org.itmo.escience.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.TwitterSequentialTypedWorkerActor.TwitterTypedWorkerTaskRequest
import org.itmo.escience.core.balancers.{Init, UpdateSlots}
import org.itmo.escience.core.osn.common.{Account, Task, TwitterAccount, TwitterTask}
import org.itmo.escience.core.osn.twitter.tasks.TwitterTaskUtil
import org.itmo.escience.dao.{KafkaUniqueSaver, KafkaUniqueSaverInfo, _}
import twitter4j.{Twitter, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

/**
  * Created by djvip on 13.08.2016.
  */
object TwitterSequentialTypedWorkerActor {

  def props(account: TwitterAccount, requestsMaxPerTask: Map[String, Int] = TwitterTaskUtil.getAllSlots()) =
    Props(new TwitterSequentialTypedWorkerActor(account, requestsMaxPerTask))

  case class TwitterTypedWorkerTaskRequest(
    freeSlots: Set[String] ,
    previousTask: Task = null
  )
}


class TwitterSequentialTypedWorkerActor(account: TwitterAccount,
                                        requestsMaxPerTask: Map[String, Int]
                                       ) extends Actor {
  val logger = Logger.getLogger(this.getClass)

  var twitter: Twitter = {
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setJSONStoreEnabled(true)
      .setOAuthConsumerKey(account.key)
      .setOAuthConsumerSecret(account.secret)
      .setOAuthAccessToken(account.token)
      .setOAuthAccessTokenSecret(account.tokenSecret)
    val twitter = new TwitterFactory(cb.build()).getInstance()
    twitter
  }

  var balancer: ActorRef = _

  /* tasktype and requests available */
  val slots = mutable.Map[String, Int]()

  val blockedTime: FiniteDuration = 15 minutes

  override def receive: Receive = {
    case task: TwitterTask =>
      logger.info(s"task = $task ${task.getClass}")

      inject(task)
      /* running task */
      task.run(twitter)

      /* slots updating */
      if (!slots.keySet.contains(task.taskType()))
        slots(task.taskType()) = requestsMaxPerTask(task.taskType()) - 1
      else
        slots(task.taskType()) -= 1

      /* asking new task */
      val freeSlots = slots.filter { case (_, requestsLeft) => requestsLeft > 0 }.keys.toSet
      balancer ! TwitterTypedWorkerTaskRequest(freeSlots, task)


    case Init() =>
      logger.info(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! TwitterTypedWorkerTaskRequest(requestsMaxPerTask.keys.toSet)

      /* updating slots each $blockedTime seconds */
      context.system.scheduler.scheduleOnce(
        blockedTime, self, UpdateSlots()
      )(context.dispatcher)

    case UpdateSlots() =>
      slots.keys.foreach { taskType =>
        slots(taskType) = requestsMaxPerTask(taskType)
      }
      logger.info(s"all slots updated $slots")

      balancer ! TwitterTypedWorkerTaskRequest(requestsMaxPerTask.keys.toSet)

      /* updating slots each $blockedTime seconds */
      context.system.scheduler.scheduleOnce(
        blockedTime, self, UpdateSlots()
      )(context.dispatcher)

    case _ =>
      throw new RuntimeException("Unknown message type!")
  }

  def inject(task: TwitterTask) {
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

}


