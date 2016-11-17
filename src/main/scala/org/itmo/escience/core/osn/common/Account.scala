package org.itmo.escience.core.osn.common

/**
  * Created by vipmax on 31.10.16.
  */
sealed trait Account

case class MockAccount() extends Account
case class TwitterAccount(key: String, secret: String, token: String, tokenSecret: String) extends Account
case class VkontakteAccount(accessToken: String) extends Account
