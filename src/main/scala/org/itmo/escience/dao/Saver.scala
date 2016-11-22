package org.itmo.escience.dao

import com.mongodb.{BasicDBObject, MongoClient}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.collection.JavaConversions._

/**
  * Created by vipmax on 18.11.16.
  */
trait Saver {
  val logger = Logger.getLogger(this.getClass)
  def save(data: Any)
}

class SaverInfo
case class MongoSaverInfo(endpoint:String, db:String, collection:String) extends SaverInfo
case class MongoSaverInfo2(endpoint:String, db:String, collection:String, collection2:String) extends SaverInfo
case class KafkaSaverInfo(endpoint:String, topic:String) extends SaverInfo
case class KafkaUniqueSaverInfo(kafkaEndpoint:String,redisEndpoint:String, topic:String) extends SaverInfo
case class RedisSaverInfo(redisEndpoint:String, prefix:String) extends SaverInfo


case class KafkaSaver(kafkaEndpoint: String, topic: String) extends Saver {
  val props = Map[String, Object]("bootstrap.servers" -> kafkaEndpoint)
  val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)

  override def save(data: Any) {
    data match {
      case d:BasicDBObject =>
        val json = d.toJson
        logger.debug(s"Needs to save BasicDBObject data $json")
        producer.send(new ProducerRecord[String,String](topic, json))

      case _ =>
        logger.debug(s"Needs to save unknown data ")
    }
  }
}

case class KafkaUniqueSaver(kafkaEndpoint: String, redisEndpoint: String, kafkaTopic: String) extends Saver {
  val kafkaProps = Map[String, Object]("bootstrap.servers" -> kafkaEndpoint)
  val kafkaProducer = new KafkaProducer[String, String](kafkaProps, new StringSerializer, new StringSerializer)

  val redisPool = new JedisPool(new JedisPoolConfig(), redisEndpoint)
  val jedis = redisPool.getResource

  override def save(data: Any) {
    data match {
      case d:BasicDBObject =>

        val key = s"$kafkaTopic-${d.getString("key", java.util.UUID.randomUUID.toString.substring(0, 20))}"
        val value = d.toJson
        logger.debug(s"Needs to save BasicDBObject data $value")

//        val rsps = jedis.setnx(key, "")
        val rsps: Long = try { jedis.setnx(key, "") } catch {case e:Exception => 1L}
        rsps match {
          case r if r <= 0 =>
            logger.debug(s"Key $key already saved to kafka")
            try { jedis.expire(key, 60) } catch {case e:Exception => 0L}


          case r if r > 0 =>
            logger.debug(s"Needs  save $key to kafka")
            kafkaProducer.send(new ProducerRecord[String,String](kafkaTopic, value))
        }

      case _ =>
        logger.debug(s"Needs to save unknown data ")
    }
  }
}


case class RedisSaver(redisEndpoint: String, collection: String, updateValue: Boolean = true) extends Saver {
  val pool = new JedisPool(new JedisPoolConfig(), redisEndpoint)

  override def save(data: Any) {
    data match {
      case d:BasicDBObject =>
        val key = s"$collection-${d.getString("key", java.util.UUID.randomUUID.toString.substring(0, 20))}"
        val value = d.toJson
        logger.debug(s"Needs to save BasicDBObject with $key and data $value")

        val jedis = pool.getResource
        val rsps = jedis.setnx(key, value)
        rsps match {
          case r if r <= 0 =>
            logger.debug(s"Key $key found in redis")
            if(updateValue) jedis.set(key, value)

          case r if r > 0 =>
            logger.debug(s"Key $key saved")
        }

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
        logger.debug(s"Needs to save BasicDBObject with key=$key and data=${d.toJson}")
        val updateResult = dao.update(collection, key, d)
        logger.debug(updateResult)

      case _ =>
        logger.debug(s"Needs to save unknown data ")
    }
  }
}

case class MongoSaver2(host: String, db: String, collectionName: String) extends Saver {
  val dao = new MongoDao(host, db)
  val collection = new MongoClient(host).getDatabase(db).getCollection(collectionName, classOf[BasicDBObject])

  override def save(data: Any) {
    data match {
      case d:BasicDBObject =>
        val key = d.getString("key", java.util.UUID.randomUUID.toString.substring(0,20))
        logger.debug(s"Needs to save BasicDBObject with key=$key and data=${d.toJson}")
        val updateResult = dao.update(collection, key, d)
        logger.debug(updateResult)

      case _ =>
        logger.debug(s"Needs to save unknown data ")
    }
  }
}
