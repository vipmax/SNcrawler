package org.itmo.escience.dao

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.UpdateOptions

/**
  * Created by vipmax on 18.11.16.
  */
object MongoUtil {
  def update(collection: MongoCollection[BasicDBObject], key:String, dbo: BasicDBObject) = {
    val k = new BasicDBObject("_id", key)
    collection.updateOne(k, new BasicDBObject("$set",dbo), new UpdateOptions().upsert(true))
  }
}
