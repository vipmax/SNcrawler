package org.itmo.escience.util

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
}
