package org.itmo.escience.core.osn.instagram

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import org.itmo.escience.core.osn.common.{InstagramTask, State}
import org.itmo.escience.dao.SaverInfo

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class InstagramPostsTask(username: String,
                              var count: Int = 10,
                              saverInfo: SaverInfo)
                             (implicit app: String) extends InstagramTask with State {

  /* state */
  var offset = "0"

  override def appname: String = app

  override def run(network: AnyRef) {

    var end = false
    var offset = this.offset

    while (!end) {

      val json = Http(s"https://www.instagram.com/$username/media/?max_id=$offset").execute().body

      val posts = try {
        val jsonResponse = JSON.parse(json).asInstanceOf[BasicDBObject]

        val tempPosts = jsonResponse.get("items").asInstanceOf[BasicDBList].toArray.
          map { case b: BasicDBObject =>
            b.append("key", b.getString("id"))
          }

        count -= tempPosts.length
        logger.debug(s"Remainig $count")

        if (jsonResponse.getString("status") != "ok" || !jsonResponse.getBoolean("more_available") || count <= 0) end = true
        offset = tempPosts.last.getString("id")

        saveState(Map("offset" -> offset))
        tempPosts
      } catch {
        case e: Exception =>
          logger.error(e)
          logger.error(json)
          Array[BasicDBObject]()
      }

      posts.foreach(logger.debug)
      logger.debug(List.fill(28)("#").mkString)


      Option(saver) match {
        case Some(s) => posts.foreach(s.save)
        case None => logger.debug(s"No saver for task $name")
      }
    }
  }

  override def name: String = s"InstagramPostsTask(query=$username)"

  override def saveState(stateParams: Map[String, Any]) {
    logger.debug(s"Saving state. stateParams=$stateParams")
    val offset = stateParams.getOrElse("offset", -1).toString
    this.offset = offset
  }
}

