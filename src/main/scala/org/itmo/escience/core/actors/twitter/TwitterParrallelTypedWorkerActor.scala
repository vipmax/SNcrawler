package org.itmo.escience.core.actors.twitter


import akka.actor.{Actor, ActorRef, Props}
import akka.routing.RoundRobinPool
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.twitter.TwitterSequentialTypedWorkerActor.TwitterTypedWorkerTaskRequest
import org.itmo.escience.core.actors.twitter.TwitterTaskExecutorActor.TaskResult
import org.itmo.escience.core.balancers.{Init, UpdateSlots}
import org.itmo.escience.core.osn.common.{TwitterAccount, TwitterTask}
import org.itmo.escience.core.osn.twitter.tasks.TwitterTaskUtil
import org.itmo.escience.dao.{KafkaUniqueSaver, KafkaUniqueSaverInfo, _}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterException, TwitterFactory}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Created by max on 08.12.16.
  */

object TwitterParrallelTypedWorkerActor {
  def props(account: TwitterAccount, requestsMaxPerTask: Map[String, Int] = TwitterTaskUtil.getAllSlots()) =
    Props(new TwitterParrallelTypedWorkerActor(account, requestsMaxPerTask))

}

class TwitterParrallelTypedWorkerActor(account: TwitterAccount,
                                       requestsMaxPerTask: Map[String, Int]) extends Actor {
  private val logger = Logger.getLogger(this.getClass)

  /* tasktype and requests available */
  private val slots = mutable.Map[String, Int](requestsMaxPerTask.toList: _*)
  /* after blockedTime all slots will be as initial */
  private val blockedTime: FiniteDuration = 15 minutes

  private var twitter: Twitter = {
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
  private var balancer: ActorRef = _

  private val twitterTaskExecutorActorPool = context.system.actorOf(
    RoundRobinPool(10).props(TwitterTaskExecutorActor.props(twitter))
  )

  override def receive: Receive = {
    case task: TwitterTask =>
      logger.debug(s"task = $task ${task.getClass}")
      injectDependencies(task)
      twitterTaskExecutorActorPool ! task


    case TaskResult(task, e) =>
      logger.info(s"TaskResult = $task ${task.getClass}")

      e match {
        case e: TwitterException if e.getRateLimitStatus.getRemaining <= 0 =>
          slots(task.taskType()) = 0
          logger.error(s"RateLimit Exception!!! ${e.getRateLimitStatus} Trying once more for task $task!!!")
          Thread.sleep(1000)
          balancer ! task

        case e: Exception =>
          logger.error(s"Ignoring exception for task $task ${e.getMessage.split("\n").mkString(" ")}")

        case _ => logger.error(s"Task is ended $task ")
      }

      /* slots updating */
      slots(task.taskType()) -= task.newRequestsCount()

      logger.debug(s"slots = $slots")

      /* asking new task */
      val freeSlots = getFreeSlots()
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

  private def getFreeSlots() = {
    slots.filter { case (_, requestsLeft) => requestsLeft > 0 }.keys.toSet
  }

  private def injectDependencies(task: TwitterTask) {
    task.logger = Logger.getLogger(s"${task.appname} ${task.name}")

    task.saverInfo match {
      case FileSaverInfo(filePath: String) =>
        logger.debug(s"Found saver {file $filePath}")
        task.saver = ConnectionManager.getFileSaver(filePath)

      case MongoSaverInfo(endpoint: String, db: String, collection: String) =>
        logger.debug(s"Found saver {mongo $endpoint, $db, $collection}")
        task.saver = ConnectionManager.getMongoSaver(endpoint, db, collection)

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




