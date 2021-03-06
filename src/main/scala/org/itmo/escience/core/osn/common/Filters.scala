package org.itmo.escience.core.osn.common

import org.joda.time.DateTime

/**
  * Created by max on 06.12.16.
  */
object Filters {
  case class TimeFilter(fromTime: DateTime = new DateTime(0), untilTime: DateTime = new DateTime())
  case class CountFilter(maxCount: Int = 100)
}
