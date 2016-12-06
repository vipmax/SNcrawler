package org.itmo.escience.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.TwitterSequentialTypedWorkerActor.TwitterTypedWorkerTaskRequest
import org.itmo.escience.core.balancers.{Init, UpdateSlots}
import org.itmo.escience.core.osn.common.{Task, TwitterAccount, TwitterTask}
import org.itmo.escience.core.osn.twitter.tasks.TwitterTaskUtil
import org.itmo.escience.dao.{KafkaUniqueSaver, KafkaUniqueSaverInfo, _}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterException, TwitterFactory}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}

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
  /* tasktype and requests available */
  val slots = mutable.Map[String, Int]()
  val blockedTime: FiniteDuration = 15 minutes

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

  override def receive: Receive = {
    case task: TwitterTask =>
      logger.info(s"task = $task ${task.getClass}")

      inject(task)
      /* running task */
      try {
        task.run(twitter)

        /* slots updating */
        if (!slots.keySet.contains(task.taskType()))
          slots(task.taskType()) = requestsMaxPerTask(task.taskType()) - task.newRequestsCount()
        else
          slots(task.taskType()) -= task.newRequestsCount()

      } catch {
        case e: TwitterException if e.getRateLimitStatus.getRemaining <= 0 =>
          slots(task.taskType()) = 0
          logger.error(s"Trying once more for task $task!!!")
          Thread.sleep(1000)
          balancer ! task

        case e: Exception =>
          logger.error(s"Ignoring exception for task $task")
      }

      logger.debug(slots)

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
        task.saver = ConnectionManager.getMongoConnection(endpoint, db, collection)

      case MongoSaverInfo2(endpoint: String, db: String, collection: String, collection2: String) =>
        logger.debug(s"Found saver {mongo $endpoint, $db, $collection}")
        task.saver = MongoSaver(endpoint, db, collection)
        task.saver2 = MongoSaver(endpoint, db, collection2)

      case KafkaSaverInfo(endpoint: String, topic: String) =>
        logger.debug(s"Found saver {kafka $endpoint, $topic}")
        task.saver = KafkaSaver(endpoint, topic)

      case KafkaUniqueSaverInfo(kafkaEndpoint: String, redisEndpoint: String, topic: String) =>
        logger.debug(s"Found saver {kafka unique $kafkaEndpoint,$redisEndpoint, $topic}")
        task.saver = KafkaUniqueSaver(kafkaEndpoint, redisEndpoint, topic)

      case _ => logger.debug("Unknown saver")
    }
  }

}


object ConnectionManager{
  private val mongoPool = mutable.HashMap[String, MongoSaver]()

  def getMongoConnection(host: String, db: String, collectionName: String) = {
    val key = s"$host $db $collectionName"
    val mongoSaver = mongoPool.getOrElse(key, MongoSaver(host, db, collectionName))
    mongoPool(key) = mongoSaver
    mongoSaver
  }
}
