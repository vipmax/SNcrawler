package org.itmo.escience.core.balancers

import akka.actor.{Actor, ActorRef, Props}
import org.apache.log4j._
import org.itmo.escience.core.osn.common.Task

import scala.collection.mutable

/**
  * Created by Nikolay on 8/29/2016.
  */
trait BaseBalancer extends Actor {
  protected val logger = Logger.getLogger(this.getClass.getName)

  protected val busyWorkers = mutable.Map[ActorRef, Boolean]()
  protected var actualTasksCount = 0

  protected val workers = mutable.MutableList[ActorRef]()

  protected def getFreeWorker(slot: String): Option[ActorRef]

  protected def addFreeWorker(freeWorker: ActorRef, workerTaskRequest: TypedTaskRequest)

  protected def enqueueTask(task: Task)

  protected def dequeueTask(workerTaskRequest: TypedTaskRequest): Option[Task]

  //TODO: possible better to remake as events
  protected def updateTokens(workerTaskRequest: TypedTaskRequest)

  protected def updateTokens(freeWorker: ActorRef, task: Task)

  override def receive: Receive = {
    case task: Task =>
      logger.debug(s"Task received $task")
      val worker = getFreeWorker("vk")

      worker match {
        case Some(w) =>
          logger.info(s"send task to launch: ${task.getClass.toString}")
          /** run task if possible */
          w ! task
          markWorkerBusy(w)
          updateTokens(w, task)
          actualTasksCount += 1

        case None =>
          logger.info(s"worker not found for task type: ${task.getClass.toString}")
          /** Add task to queue */
          enqueueTask(task)
          actualTasksCount += 1
      }


    case workerTaskRequest: TypedTaskRequest =>
      logger.debug(s"WorkerTaskRequest slots=(${workerTaskRequest.tokens}) worker=$sender")

      // the worker just updates information about its state
      // probably due to "account reloading"
      if (workerTaskRequest.previousTask == null) {
        updateTokens(workerTaskRequest)
      } else {
        markWorkerIdle(sender)
        actualTasksCount -= 1
      }

      if (!workerIsBusy(workerTaskRequest.worker)) {
        val maybeTask = dequeueTask(workerTaskRequest)

        maybeTask match {
          case Some(task) =>
            logger.info(s"Found app=${task.appname} for task request. Sending  to $sender")
            sender ! task
            markWorkerBusy(sender)
            // update info about tokens
            updateTokens(workerTaskRequest)

          case None =>
            logger.info(s"Task not found for request ")
            addFreeWorker(sender, workerTaskRequest)
            updateTokens(workerTaskRequest)
        }
      }

    case _ => throw new Exception(s"Unknown type of message")
  }

  protected def markWorkerBusy(worker: ActorRef) = {
    busyWorkers(worker) = true
  }

  protected def markWorkerIdle(worker: ActorRef) = {
    busyWorkers(worker) = false
  }

  protected def workerIsBusy(worker: ActorRef): Boolean = {
    busyWorkers.getOrElse(worker, false)
  }

  private def spawnWorker(props:Props, count: Int = 1) = {
    val workers = 0 until count map { i =>
      context.system.actorOf(props, name = s"sequentialWorker_$i")
    }
    workers.foreach { w => w ! Init() }
    workers.foreach { w => busyWorkers.put(w, false) }
    workers
  }

}


case class Init()
case class UpdateSlots()

case class TypedTaskRequest(tokens: collection.mutable.Map[String, Int],
                            previousTask: Task = null,
                            worker: ActorRef)

case class WorkerIsReady(worker: ActorRef)


case class App(name:String, var quantumCount:Int = 1, var quantumLeft:Int = 1,
               var avgExecutionTime:Double = -1){

  private val tasksByType: mutable.LinkedHashMap[String, mutable.LinkedHashSet[Task]] = mutable.LinkedHashMap[String, mutable.LinkedHashSet[Task]]()
  private val taskList: mutable.LinkedHashSet[Task] = mutable.LinkedHashSet[Task]()

  /*
   required for two-level quantum typed balancer
   */
  private var _currtaskTypeIndex: Int = 0

  def addTask(task:Task) {
    val taskType = task.getClass.toString
    if (!tasksByType.contains(taskType)) {
      tasksByType(taskType) = mutable.LinkedHashSet[Task]()
    }
    tasksByType(taskType).add(task)
    taskList.add(task)
  }

  def getTaskByType(taskType: String): Option[Task] = {
    tasksByType.get(taskType).map {x => x.find(_ => true).get}
  }

  def removeTask(task:Task): Unit = {
    val taskType = task.getClass.toString
    tasksByType(taskType).remove(task)
    if (tasksByType(taskType).isEmpty){
      tasksByType.remove(taskType)
      // we don't need to move currTaskTypeIndex because it is already "moved"
      // but we need to check for end of sequence
      _currtaskTypeIndex = if (_currtaskTypeIndex < tasksByType.size) _currtaskTypeIndex else 0
    }
    taskList.remove(task)
  }

  def tasks: List[Task] = taskList.toList

  def taskTypes() = tasksByType.keys.toList

  def updateCurrTaskTypeIndex(index:Int) {
    _currtaskTypeIndex = index
  }

  def currTaskTypeIndex = _currtaskTypeIndex

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case App(n,_,_,_) if n == name => true
      case _ => false
    }
  }

  override def hashCode(): Int = name.hashCode
}
