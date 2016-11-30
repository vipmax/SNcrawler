package org.itmo.escience.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.VkSimpleWorkerActor.VkSimpleWorkerTaskRequest
import org.itmo.escience.core.balancers.{Init, TypedTaskRequest}
import org.itmo.escience.core.osn.common.VkontakteTask
import org.itmo.escience.dao._
import twitter4j.{Twitter, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder

import scala.collection.mutable
import scala.collection.mutable._
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
/**
  * Created by vipmax on 14.11.16.
  */
object VkSimpleWorkerActor {
  def props() = Props[VkSimpleWorkerActor]

  case class VkSimpleWorkerTaskRequest(task: VkontakteTask)
}

class VkSimpleWorkerActor() extends Actor {
  val logger = Logger.getLogger(this.getClass)

  var balancer: ActorRef = _

  override def receive: Receive = {
    case task: VkontakteTask =>
      logger.debug(s"task = $task ${task.getClass}")

      task.logger = Logger.getLogger(s"${task.appname} ${task.name}")

      task.saverInfo match {
        case MongoSaverInfo(endpoint:String, db:String, collection:String) =>
          logger.debug(s"Found saver {mongo $endpoint, $db, $collection}")
          task.saver = new MongoSaver(endpoint, db, collection)

        case MongoSaverInfo2(endpoint:String, db:String, collection:String, collection2:String) =>
          logger.debug(s"Found saver {mongo $endpoint, $db, $collection}")
          task.saver = new MongoSaver(endpoint, db, collection)
          task.saver2 = new MongoSaver(endpoint, db, collection2)

        case KafkaSaverInfo(endpoint:String, topic:String) =>
          logger.debug(s"Found saver {kafka $endpoint, $topic}")
          task.saver = new KafkaSaver(endpoint, topic)

        case KafkaUniqueSaverInfo(kafkaEndpoint:String,redisEndpoint:String, topic:String) =>
          logger.debug(s"Found saver {kafka unique $kafkaEndpoint,$redisEndpoint, $topic}")
          task.saver = new KafkaUniqueSaver(kafkaEndpoint, redisEndpoint, topic)

        case _ => logger.debug("Unknown saver")
      }

      task.run(null)

      balancer ! VkSimpleWorkerTaskRequest(task)


    case Init() =>
      logger.debug(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! VkSimpleWorkerTaskRequest(null)

    case _ =>
      throw new RuntimeException("World is burning!!")
  }

}



