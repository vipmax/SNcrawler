package org.itmo.escience.core.actors.twitter

import akka.actor.{Actor, Props}
import org.apache.log4j.Logger
import org.itmo.escience.core.actors.twitter.TwitterTaskExecutorActor.TaskResult
import org.itmo.escience.core.osn.common.TwitterTask
import twitter4j.Twitter

/**
  * Created by djvip on 14.08.2016.
  */


object TwitterTaskExecutorActor {
  def props(twitter:Twitter) = Props(new TwitterTaskExecutorActor(twitter))
  case class TaskResult(task: TwitterTask, e: Exception)
}

class TwitterTaskExecutorActor(twitter:Twitter) extends Actor {
  val logger = Logger.getLogger(this.getClass)

  override def receive: Receive = {
    case task: TwitterTask =>
      try {
        /* running task */
        task.run(twitter)
        sender ! TaskResult(task, null)
      } catch { case e: Exception =>
          logger.error(s"Ignoring exception for task $task $e")
          sender ! TaskResult(task, e)
      }

    case _ =>
      throw new RuntimeException("Unknown message type!")
  }
}