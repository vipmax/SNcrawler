package org.itmo.escience.core.osn.twitter.tasks

import java.util

import org.itmo.escience.core.osn.common.{State, TwitterTask}
import org.itmo.escience.dao.SaverInfo
import twitter4j._

import scala.collection.JavaConversions._

/**
  * Created by vipmax on 29.11.16.
  */
case class TwitterSearchPostsTask(query: Query, var count: Int = -1, saverInfo: SaverInfo)(implicit app: String) extends TwitterTask with State {

  /* state */
  var offset = 1L
  var _newRequestsCount = 0

  override def appname: String = app

  override def run(network: AnyRef) {
    network match {
      case twitter: Twitter => extract(twitter)
      case _ => logger.debug("No TwitterTemplate object found")
    }
  }

  def extract(twitter: Twitter) {
    var searchResult = twitter.search().search(query)
    _newRequestsCount += 1

    val tweets = searchResult.getTweets
    tweets.foreach { t => logger.debug(t.getCreatedAt) }
    count -= tweets.length
    logger.debug(List.fill(28)("#").mkString)
    saveTweets(tweets)

    while (searchResult.hasNext && (count > 0 || count == -1)) {
      searchResult = twitter.search().search(searchResult.nextQuery())
      _newRequestsCount += 1
      val tweets = searchResult.getTweets
      tweets.foreach { t => logger.debug(t.getCreatedAt) }
      count -= tweets.length

      saveTweets(tweets)
      logger.debug(List.fill(28)("#").mkString)
    }
  }

  private def saveTweets(tweets: util.List[Status]) = {
    val data = TwitterTaskUtil.mapStatuses(tweets.toList)

    Option(saver) match {
      case Some(s) => data.foreach(s.save)
      case None => logger.debug(s"No saver for task $name")
    }
  }

  override def name: String = s"TwitterSearchPostsTask(query=${query.getQuery})"

  override def saveState(stateParams: Map[String, Any]) {
    logger.debug(s"Saving state. stateParams=$stateParams")
    val offset = stateParams.getOrElse("offset", -1).toString.toLong
    this.offset = offset
  }

  override def newRequestsCount() = {
    val returned = _newRequestsCount
    _newRequestsCount = 0
    returned
  }
}
