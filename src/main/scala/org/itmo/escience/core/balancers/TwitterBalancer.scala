package org.itmo.escience.core.balancers

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.TwitterSequentialTypedWorkerActor.TwitterTypedWorkerTaskRequest
import org.itmo.escience.core.actors.{TwitterSimpleWorkerActor, VkSimpleWorkerActor}
import org.itmo.escience.core.osn.common.{Task, TwitterTask, VkontakteTask}
import org.itmo.escience.util.Util
import org.itmo.escience.util.Util.{Continue, Stop, _}

import scala.collection.mutable

/**
  * Created by vipmax on 29.11.16.
  */
object TwitterBalancer {
  def main(args: Array[String]) {
    val ip = "127.0.0.1"

    val akkaSystemName = """TwitterBalancer"""
    val config: String = s"""
      akka {
          actor {
            provider = "akka.remote.RemoteActorRefProvider"
          }
          remote {
            log-remote-lifecycle-events = off
            netty.tcp {
              hostname = "$ip"
              port = 2551
            }
          }
          serializers {
            java = "akka.serialization.JavaSerializer"
          }
      }
     """

    val actorSystem = ActorSystem(akkaSystemName, ConfigFactory.parseString(config))
    val balancer = actorSystem.actorOf(Props[TwitterBalancer], "balancer")

    val accounts = Util.getTwitterAccounts().take(10)

    accounts foreach { account =>
      actorSystem.actorOf(TwitterSimpleWorkerActor.props(account)).tell(Init(), balancer)
    }
  }

}


class TwitterBalancer extends Actor {
  val logger = Logger.getLogger(this.getClass)

  val freeWorkers = mutable.Map[String, mutable.Set[ActorRef]]()
  val apps = new mutable.MutableList[App]()

  var currentAppIndex = 0
  var actualTasksCount = 0
  val taskCounters = mutable.Map[String, Int]()

  override def receive: Receive = {
    case workerTaskRequest: TwitterTypedWorkerTaskRequest =>
      logger.debug(s"Got ${workerTaskRequest.getClass.getSimpleName}($workerTaskRequest)")

      val maybeTask = dequeueTask(workerTaskRequest)

      maybeTask match {
        case Some(task) =>
          logger.info(s"Found task=${task.name} for workerTaskRequest. Sending to worker $sender")
          sender ! task

        case None =>
          logger.info(s"Task not found for workerTaskRequest $workerTaskRequest")
          addFreeWorker(sender, workerTaskRequest)
      }


    case task:TwitterTask  =>
      logger.debug(s"Got ${task.getClass.getSimpleName}(${task.name})")

      val freeWorker = getFreeWorker(task.taskType())

      freeWorker match {
        case Some(worker) =>
          logger.info(s"Sending task ${task.name} to worker $worker")

          worker ! task
          removeFreeWorker(worker,task.taskType())
          actualTasksCount += 1

        case None =>
          logger.info(s"freeWorker not found for task type: ${task.taskType()}")

          enqueueTask(task)
          actualTasksCount += 1
      }

    case _ => throw new Exception(s"Unknown type of message")
  }


  def getFreeWorker(taskType: String): Option[ActorRef] = {
    if (!freeWorkers.contains(taskType))
      return None

    val workers = freeWorkers(taskType)
    if (workers.nonEmpty) {
      val worker = workers.head
      workers -= worker

      // if take the actor for executing a task,
      // we remove it from anywhere else cause it blocks the actot
      for ((slot, workers) <- freeWorkers) {
        workers.remove(worker)
      }

      Option(worker)
    }
    else {
      None
    }
  }

  /* for each  free slot adding freeWorker */
  def addFreeWorker(freeWorker: ActorRef, workerTaskRequest: TwitterTypedWorkerTaskRequest) = {
    workerTaskRequest.freeSlots.foreach{ case freeSlot =>
      if (!freeWorkers.contains(freeSlot)) freeWorkers.put(freeSlot, mutable.Set[ActorRef]())
      freeWorkers(freeSlot) += freeWorker
    }
  }

  def enqueueTask(task: Task) = {
    val app = App(task.appname)

    if (!apps.contains(app)) {
      app.addTask(task)
      apps += app
    }
    else {
      val currentApp = apps.find(_.equals(app)).get
      currentApp.addTask(task)
    }

    val taskType = task.getClass.toString
    if (!taskCounters.contains(taskType)){
      taskCounters.put(taskType, 0)
    }
    taskCounters(taskType) += 1
  }

  def dequeueTask(workerTaskRequest: TwitterTypedWorkerTaskRequest): Option[Task] = {
    val appAndTasks = findApp(workerTaskRequest)

    val task = appAndTasks match {
      case Some((app, availableTaskTypes)) =>
        val task = findTask(app, availableTaskTypes)
        task

      case None =>
        None
    }

    // update appropriate counters
    task match {
      case Some(t) =>
        val (app, _) = appAndTasks.get
        app.removeTask(t)

        val taskType = t.getClass.toString
        taskCounters(taskType) -= 1
      case None =>
    }

    task
  }

  def findApp(taskRequest: TwitterTypedWorkerTaskRequest): Option[(App, Set[String])] = {
    if (apps.isEmpty)
      return None

    val (currIndex, app) = ringLoop(apps, start = currentAppIndex) { app =>
      // check if tokens are matching
      val availableTaskTypes = app.taskTypes().toSet.intersect(taskRequest.freeSlots)
      if (availableTaskTypes.nonEmpty) {
        Stop((app, availableTaskTypes))
      } else {
        Continue
      }
    }

    currentAppIndex = if (currIndex + 1 < apps.length) currIndex + 1 else 0
    app
  }

  def findTask(app: App, availableTaskTypes: Set[String]): Option[Task] = {
    val sortedTaskTypes = app.taskTypes()
    val curr = app.currTaskTypeIndex

    val (currIndex, task) = ringLoop(sortedTaskTypes, start = curr) { taskType =>
      // check if tokens are matching
      if (availableTaskTypes.contains(taskType)) {
        val task = app.getTaskByType(taskType)
        task match {
          case Some(t) =>
            Stop(t)
          case None =>
            Continue
        }
      } else {
        Continue
      }
    }

    val newCurrIndex = if (currIndex + 1 < sortedTaskTypes.length) currIndex + 1 else 0
    app.updateCurrTaskTypeIndex(newCurrIndex)
    task
  }

  def removeFreeWorker(worker: ActorRef, taskType: String) = {
    freeWorkers(taskType) -= worker
  }
}


