package org.itmo.escience.core.balancers

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.VkSimpleWorkerActor
import org.itmo.escience.core.actors.VkSimpleWorkerActor.VkSimpleWorkerTaskRequest
import org.itmo.escience.core.osn.common.{Task, VkontakteTask}
import org.itmo.escience.core.osn.vkontakte.tasks.{VkUserPostsTask, VkUserProfileTask}
import org.itmo.escience.util.Util.{Continue, Stop, _}
import org.itmo.escience.dao.{KafkaSaver, KafkaUniqueSaver, MongoSaver, RedisSaver}

import scala.collection.mutable

/**
  * Created by vipmax on 14.11.16.
  */
object VkBalancer {
  def main(args: Array[String]) {
    val actorSystem = ActorSystem("VkBalancer")
    val balancer = actorSystem.actorOf(Props[VkBalancer])

    1 until 10 foreach { i=>
      actorSystem.actorOf(Props[VkSimpleWorkerActor]).tell(Init(), balancer)
    }

    implicit val appname = "testApp"

//    balancer ! new VkUserProfileTask("1", MongoSaver("192.168.13.133","test_db","test_collection"))
    balancer ! new VkUserProfileTask("1", KafkaUniqueSaver("192.168.13.133:9092","localhost", "users"))

  }
}

class VkBalancer extends Actor {
  val logger = Logger.getLogger(this.getClass)

  val freeWorkers = mutable.Map[String, mutable.Set[ActorRef]]()
  val apps = new mutable.MutableList[App]()

  var currentAppIndex = 0
  var actualTasksCount = 0
  val taskCounters = mutable.Map[String, Int]()


  override def receive: Receive = {
    case workerTaskRequest: VkSimpleWorkerTaskRequest =>
      logger.debug(s"Got VkSimpleWorkerTaskRequest($workerTaskRequest)")

      val maybeTask = dequeueTask(workerTaskRequest)

      maybeTask match {
        case Some(task) =>
          logger.info(s"Found task=${task.name} for workerTaskRequest. Sending to worker $sender")
          sender ! task

        case None =>
          logger.info(s"Task not found for workerTaskRequest $workerTaskRequest")
          addFreeWorker(sender, workerTaskRequest)
      }


    case task:VkontakteTask  =>
      logger.debug(s"Got VkontakteTask(${task.name})")

      val freeWorker = getFreeWorker("vk")

      freeWorker match {
        case Some(worker) =>
          logger.info(s"Sending task ${task.name} to worker $worker")

          worker ! task
          removeFreeWorker(worker)
          actualTasksCount += 1

        case None =>
          logger.info(s"freeWorker not found for task type: ${task.name}")

          enqueueTask(task)
          actualTasksCount += 1
      }

    case _ => throw new Exception(s"Unknown type of message")
  }


  def getFreeWorker(slot: String): Option[ActorRef] = {
    if (!freeWorkers.contains(slot))
      return None

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

  def addFreeWorker(freeWorker: ActorRef, workerTaskRequest: VkSimpleWorkerTaskRequest) = {
    if (!freeWorkers.contains("vk")) freeWorkers.put("vk", mutable.Set[ActorRef]())
    freeWorkers("vk") += freeWorker
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

  def dequeueTask(workerTaskRequest: VkSimpleWorkerTaskRequest): Option[Task] = {
    val appAndTasks = findApp()

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

  def findApp(): Option[(App, Set[String])] = {
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

  def removeFreeWorker(worker: ActorRef) = {
    freeWorkers("vk") -= worker
  }
}

