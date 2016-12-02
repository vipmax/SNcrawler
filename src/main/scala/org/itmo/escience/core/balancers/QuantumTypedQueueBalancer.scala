package org.itmo.escience.core.balancers

import akka.actor.{Actor, ActorRef}
import org.itmo.escience.core.osn.common.Task
import org.itmo.escience.util.Util._

import scala.collection.mutable

/**
  * Created by Nikolay on 8/30/2016.
  */

class QuantumTypedQueueBalancer extends Actor with BaseBalancer {

  protected val taskCounters = mutable.Map[String, Int]()
  protected val freeWorkers = mutable.Map[String, mutable.Set[ActorRef]]()
  protected val apps: mutable.MutableList[App] = new mutable.MutableList[App]()
  protected var currentAppIndex = 0

  override protected def getFreeWorker(slot: String): Option[ActorRef] = {
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

  override protected def addFreeWorker(freeWorker: ActorRef, workerTaskRequest: TypedTaskRequest): Unit = {
    val freeTokens = workerTaskRequest.tokens.filter { case (_, callLeft) => callLeft > 0 }.keys.toSet
    freeTokens.foreach { freeToken =>
      if (!freeWorkers.contains(freeToken)){
        freeWorkers.put(freeToken, mutable.Set[ActorRef]())
      }
      freeWorkers(freeToken) += freeWorker
    }

    logger.info(s"added ${freeTokens.size} freeTokens $freeTokens")
  }

  override protected def enqueueTask(task: Task): Unit = {
    val app = App(task.appname)

    val contains = apps.contains(app)
    if (!contains) {
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

  override protected def dequeueTask(workerInfo: TypedTaskRequest): Option[Task] = {
    val appAndTasks = findApp(workerInfo.tokens)

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

  protected def findApp(tokens: mutable.Map[String, Int]): Option[(App, Set[String])] = {

    if (apps.isEmpty)
      return None

    // look for an app in a cycle
    val freeTokens = tokens.filter { case (_, callLeft) => callLeft > 0 }.keys.toSet

    val (currIndex, app) = ringLoop(apps, start = currentAppIndex) { app =>
      // check if tokens are matching
      val availableTaskTypes = app.taskTypes().toSet.intersect(freeTokens)
      if (availableTaskTypes.nonEmpty) {
        Stop((app, availableTaskTypes))
      } else {
        Continue
      }
    }

    currentAppIndex = if (currIndex + 1 < apps.length) currIndex + 1 else 0

    app
  }

  protected def findTask(app: App, availableTaskTypes: Set[String]): Option[Task] = {
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

  override protected def updateTokens(workerTaskRequest: TypedTaskRequest): Unit = {
  }

  override protected def updateTokens(freeWorker: ActorRef, task: Task): Unit = {}
}
