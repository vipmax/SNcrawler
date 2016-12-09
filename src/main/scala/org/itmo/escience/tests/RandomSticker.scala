package org.itmo.escience.tests

import scala.util.Random
import scalaj.http.Http

/**
  * Created by max on 08.12.16.
  */
object RandomStickerSender {
  def main(args: Array[String]){
    while (true) {
      Http("https://api.vk.com/method/messages.send")
        .param("access_token", "7c5ad6ee7c5fbddea591e9e6cc2c50106ea650f70a2be4b0f9886653c7682f1cf11039ce1a78817a53e24")
        .param("user_id", "212116411")
        .param("sticker_id", Random.nextInt(1000).toString)
        .param("v", "5.8")
        .execute()

      Thread.sleep(Random.nextInt(10000))
    }
  }
}
