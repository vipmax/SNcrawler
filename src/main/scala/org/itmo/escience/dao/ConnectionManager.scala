package org.itmo.escience.dao

import scala.collection.mutable

/**
  * Created by max on 06.12.16.
  */
object ConnectionManager{
  private val mongoPool = mutable.HashMap[String, MongoSaver]()
  private val filePool = mutable.HashMap[String, FileSaver]()

  def getMongoSaver(host: String, db: String, collectionName: String) = {
    this.synchronized {
      val key = s"$host $db $collectionName"
      val mongoSaver = mongoPool.getOrElse(key, MongoSaver(host, db, collectionName))
      mongoPool(key) = mongoSaver
      mongoSaver
    }
  }

  def getFileSaver(file: String) = {
    this.synchronized {
      val key = file
      val fileSaver = filePool.getOrElse(key, FileSaver(file))
      filePool(key) = fileSaver
      fileSaver
    }
  }
}
