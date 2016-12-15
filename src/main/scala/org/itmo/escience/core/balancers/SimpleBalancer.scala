package org.itmo.escience.core.balancers

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.SimpleWorkerActor
import org.itmo.escience.core.actors.SimpleWorkerActor.SimpleWorkerTaskRequest
import org.itmo.escience.core.osn.common.Task
import org.itmo.escience.util.Util.{Continue, Stop, _}

import scala.collection.mutable
/**
  * Created by max on 02.12.16.
  */


object SimpleBalancer {
  def main(args: Array[String]) {
    val ip = "127.0.0.1"

    val akkaSystemName = """VkBalancer"""
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
    val balancer = actorSystem.actorOf(Props[SimpleBalancer], "balancer")

    1 until 10 foreach { i=>
      actorSystem.actorOf(Props[SimpleWorkerActor]).tell(Init(), balancer)
    }
  }
}

class SimpleBalancer extends Actor {
  val logger = Logger.getLogger(this.getClass)

  val freeWorkers = mutable.Map[String, mutable.Set[ActorRef]]()
  val apps = new mutable.MutableList[App]()
  val taskCounters = mutable.Map[String, Int]()
  var currentAppIndex = 0
  var actualTasksCount = 0

  override def receive: Receive = {
    case workerTaskRequest: SimpleWorkerTaskRequest =>
      logger.debug(s"Got SimpleWorkerTaskRequest($workerTaskRequest)")

      val maybeTask = dequeueTask(workerTaskRequest)

      maybeTask match {
        case Some(task) =>
          logger.info(s"Found task=${task.name} for workerTaskRequest. Sending to worker $sender")
          sender ! task

        case None =>
          logger.info(s"Task not found for workerTaskRequest $workerTaskRequest")
          addFreeWorker(sender, workerTaskRequest)
      }


    case task: Task =>
      logger.debug(s"Got Task(${task.name})")

      val freeWorker = getFreeWorker(task.taskType())

      freeWorker match {
        case Some(worker) =>
          logger.info(s"Sending task ${task.name} to worker $worker")

          worker ! task
          removeFreeWorker(worker, task.taskType())
          actualTasksCount += 1

        case None =>
          logger.info(s"freeWorker not found for task type: ${task.taskType()}")

          enqueueTask(task)
          actualTasksCount += 1
      }

    case _ => throw new Exception(s"Unknown type of message")
  }


  def getFreeWorker(slot: String): Option[ActorRef] = {
    if(freeWorkers.contains("anytask")) return getAndRemoveFreeWorker("anytask")
    if (!freeWorkers.contains(slot))    return None

    val freeWorker = getAndRemoveFreeWorker(slot)
    return freeWorker
  }

  private def getAndRemoveFreeWorker(slot: String) = {
    val workers = freeWorkers(slot)
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

  def addFreeWorker(freeWorker: ActorRef, workerTaskRequest: SimpleWorkerTaskRequest) = {
    val tt = Option(workerTaskRequest.task) match {
      case Some(t) => t.taskType()
      case None => "anytask"
    }

    if (!freeWorkers.contains(tt)) freeWorkers.put(tt, mutable.Set[ActorRef]())
    freeWorkers(tt) += freeWorker
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

  def dequeueTask(workerTaskRequest: SimpleWorkerTaskRequest): Option[Task] = {
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

    // calculate probabilities and choose the task
    // returns one of the tasks back
    task
  }

  def findApp(taskRequest: SimpleWorkerTaskRequest): Option[(App, Set[String])] = {
    if (apps.isEmpty)
      return None

    val (currIndex, app) = ringLoop(apps, start = currentAppIndex) { app =>
      // check if tokens are matching
      val availableTaskTypes = app.taskTypes().toSet
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

  def removeFreeWorker(worker: ActorRef, tasktype:String) = {
    if(freeWorkers.contains(tasktype))freeWorkers(tasktype) -= worker
  }
}
