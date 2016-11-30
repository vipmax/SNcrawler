package org.itmo.escience.core.osn.twitter.tasks

/**
  * Created by vipmax on 30.11.16.
  */

/* Look at https://dev.twitter.com/rest/public/rate-limits */
object TwitterTaskUtil {
  def getAllSlots() = {
    val slots = Map(
      "TwitterFollowersTask" -> 15,
      "TwitterPostsTask" -> 900
    )
    println(slots)
    slots
  }
  def getAllTasks() = {
    val tasks = Set(
      "TwitterFollowersTask",
      "TwitterPostsTask"
    )
    println(tasks)
    tasks
  }
}
