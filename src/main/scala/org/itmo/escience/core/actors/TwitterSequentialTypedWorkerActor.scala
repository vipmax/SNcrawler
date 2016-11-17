package org.itmo.escience.core.actors

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.TwitterSequentialTypedWorkerActor.TwitterSequentialTypedWorkerTaskRequest
import org.itmo.escience.core.balancers.{Init, UpdateSlots}
import org.itmo.escience.core.osn.common.{Account, Task, TwitterTask}
import twitter4j.Twitter

import scala.collection.mutable._
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

/**
  * Created by djvip on 13.08.2016.
  */
object TwitterSequentialTypedWorkerActor {

  def props(account: Account, requestsMaxPerTask: Map[Class[_ <: TwitterTask], Int]) =
    Props(new TwitterSequentialTypedWorkerActor(account, requestsMaxPerTask))

  case class TwitterSequentialTypedWorkerTaskRequest(
    usedSlots: List[Class[_ <: TwitterTask]] = List[Class[_ <: TwitterTask]](),
    previousTask: Task = null
  )
}


class TwitterSequentialTypedWorkerActor(account: Account, requestsMaxPerTask: Map[Class[_ <: TwitterTask], Int]) extends Actor {
  val logger = Logger.getLogger(this.getClass)

  var twitter: Twitter = _
  var balancer: ActorRef = _

  val requestsAvailablePerTaskType = Map[Class[_ <: TwitterTask], Int]()

  val blockedTime: FiniteDuration = 15 seconds

  override def receive: Receive = {
    case task: TwitterTask =>
      logger.info(s"task = $task ${task.getClass}")

      /* running task */
      task.run(twitter)

      /* slots updating */
      if (!requestsAvailablePerTaskType.keySet.contains(task.getClass))
        requestsAvailablePerTaskType(task.getClass) = requestsMaxPerTask(task.getClass) - 1
      else
        requestsAvailablePerTaskType(task.getClass) -= 1

      /* asking new task */
      val usedSlots = requestsAvailablePerTaskType.filter { case (_, requestsLeft) => requestsLeft <= 0 }.keys
      balancer ! TwitterSequentialTypedWorkerTaskRequest(usedSlots.toList, task)


    case Init() =>
      logger.info(s"Init balancer with sender=$sender")
      balancer = sender
      balancer ! TwitterSequentialTypedWorkerTaskRequest()

      /* updating slots each $blockedTime seconds */
      context.system.scheduler.scheduleOnce(
        blockedTime, self, UpdateSlots()
      )(context.dispatcher)

    case UpdateSlots() =>
      requestsAvailablePerTaskType.keys.foreach { taskType => requestsAvailablePerTaskType(taskType) = requestsMaxPerTask(taskType) }
      logger.info(s"all slots updated $requestsAvailablePerTaskType")

      balancer ! TwitterSequentialTypedWorkerTaskRequest()

      /* updating slots each $blockedTime seconds */
      context.system.scheduler.scheduleOnce(
        blockedTime, self, UpdateSlots()
      )(context.dispatcher)

    case _ =>
      throw new RuntimeException("World is burning!!")
  }

}


