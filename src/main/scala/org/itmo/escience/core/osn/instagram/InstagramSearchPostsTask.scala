package org.itmo.escience.core.osn.instagram

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import org.itmo.escience.core.osn.common.{InstagramTask, State}
import org.itmo.escience.dao.SaverInfo

import scalaj.http.Http

/**
  * Created by vipmax on 31.10.16.
  */
case class InstagramSearchPostsTask(query: String,
                                    var count: Int = 10,
                                    saverInfo: SaverInfo)
                                   (implicit app: String) extends InstagramTask with State {

  /* state */
  var offset = "-1"

  override def appname: String = app

  override def run(network: AnyRef) {

    var end = false
    var offset = this.offset

    while (!end) {

      val request = offset match {
        case "-1" => Http(s"https://www.instagram.com/explore/tags/$query/?__a=1")
        case next: String => Http(s"https://www.instagram.com/explore/tags/$query/?__a=1").param("max_id", next)
      }

      val json = request.execute().body

      val posts = try {
        val jsonResponse = JSON.parse(json).asInstanceOf[BasicDBObject]

        val media = jsonResponse.get("tag").asInstanceOf[BasicDBObject]
          .get("media").asInstanceOf[BasicDBObject]

        val pageInfo = media.get("page_info").asInstanceOf[BasicDBObject]

        val tempPosts = media.get("nodes").asInstanceOf[BasicDBList]
          .toArray.map { case b: BasicDBObject =>
          b.append("key", b.getString("id"))
        }

        count -= tempPosts.length
        logger.debug(s"Remainig $count")

        end = !pageInfo.getBoolean("has_next_page") || count <= 0
        offset = pageInfo.getString("end_cursor")
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

  override def name: String = s"InstagramSearchPostsTask(query=$query)"

  override def saveState(stateParams: Map[String, Any]) {
    logger.debug(s"Saving state. stateParams=$stateParams")
    val offset = stateParams.getOrElse("offset", -1).toString
    this.offset = offset
  }
}

