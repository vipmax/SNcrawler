package org.itmo.escience.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.VkSimpleWorkerActor.VkSimpleWorkerTaskRequest
import org.itmo.escience.core.balancers.{Init, TypedTaskRequest}
import org.itmo.escience.core.osn.common.VkontakteTask

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
      logger.info(s"task = $task ${task.getClass}")

      task.run(null)

      balancer ! VkSimpleWorkerTaskRequest(task)


    case Init() =>
//      logger.info(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! VkSimpleWorkerTaskRequest(null)

    case _ =>
      throw new RuntimeException("World is burning!!")
  }
}



