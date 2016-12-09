package org.itmo.escience.util

import java.io.File

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import org.itmo.escience.core.osn.common.TwitterAccount

import scala.io.Source

/**
  * Created by vipmax on 31.10.16.
  */
object Util {
  def ringLoop[B, T](loopable:Seq[B], start:Int)(codeblock: (B) => Result[T]): (Int, Option[T]) = {
    var i = start
    if (i >= loopable.length)
      throw new RuntimeException("Start is incorrect")
    do {
      codeblock(loopable(i)) match {
        case Stop(result: T) =>
          return (i, Some(result))
        case Continue =>
          i = if (i + 1 < loopable.length) i + 1 else 0
      }
    }
    while (i != start)

    (i, None)
  }
  abstract class Result[+T]
  case class Stop[B](result:B) extends Result[B]
  case object Continue extends Result[Nothing]


  def getTwitterAccounts() = {
    val creds = new File(s"resources/twitterCreds")
      .listFiles()
      .flatMap(file => {
        Source.fromFile(file)
          .getLines()
          .filter(_.length > 10)
          .grouped(4)
          .map(group => TwitterAccount(group.head.split("key=")(1),
            group(1).split("secret=")(1),
            group(2).split("token=")(1),
            group(3).split("token_secret=")(1)
          ))
      })
    creds
  }


  val conf = JSON.parse(Source.fromFile("resources/crawler_config.json").mkString("")).asInstanceOf[BasicDBObject]
  println(conf)

}
