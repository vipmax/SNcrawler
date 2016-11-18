package org.itmo.escience.dao

import com.mongodb.{BasicDBObject, MongoClient}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.log4j.Logger

/**
  * Created by vipmax on 18.11.16.
  */
trait Saver {
  val logger = Logger.getLogger(this.getClass)
  def save(data: Any)
}


case class KafkaSaver(kafkaEndpoint: String, topic: String) extends Saver {
  val props = Map[String, Object]("bootstrap.servers" -> kafkaEndpoint)
  val producer = new KafkaProducer[Array[Byte], Array[Byte]](props, new StringSerializer, new StringSerializer)

  override def save(data: Any) {
    data match {
      case d:BasicDBObject =>
        logger.debug(s"Needs to save BasicDBObject data ${d.toJson.substring(0, 100)}")

      case _ =>
        logger.debug(s"Needs to save unknown data ")
    }
  }
}

case class MongoSaver(host: String, db: String, collectionName: String) extends Saver {
  val dao = new MongoDao(host, db)
  val collection = new MongoClient(host).getDatabase(db).getCollection(collectionName, classOf[BasicDBObject])

  override def save(data: Any) {
    data match {
      case d:BasicDBObject =>
        val key = d.getString("key", java.util.UUID.randomUUID.toString.substring(0,20))
        logger.debug(s"Needs to save BasicDBObject with key=$key and data=${d.toJson.substring(0, 100)}")
        val updateResult = dao.update(collection, key, d)
        logger.debug(updateResult)

      case _ =>
        logger.debug(s"Needs to save unknown data ")
    }
  }
}
