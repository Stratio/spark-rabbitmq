/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.receiver

import scala.reflect.ClassTag

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object RabbitMQUtils {

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param ssc                StreamingContext object
   * @param rabbitMQHost       RabbitMQ host string in the format of <host:port>,<host:port>,...
   * @param rabbitMQQueueName  Queue to subscribe to
   * @param storageLevel       RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStreamFromAQueue(ssc: StreamingContext,
                   rabbitMQHost: String,
                   rabbitMQQueueName: String,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
                   vhost: String = "/",
                   username: String = "guest",
                   password: String = "guest"
                    ): ReceiverInputDStream[String] = {
    new RabbitMQInputDStream(
      ssc,
      Some(rabbitMQQueueName),
      rabbitMQHost,
      None,
      Seq(),
      storageLevel,
      vhost,
      username,
      password)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param jssc               JavaStreamingContext object
   * @param rabbitMQHost       RabbitMQ host string in the format of <host:port>,<host:port>,...
   * @param rabbitMQQueueName  Queue to subscribe to
   * @param storageLevel       RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createJavaStreamFromAQueue(jssc: JavaStreamingContext,
                   rabbitMQHost: String,
                   rabbitMQQueueName: String,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
                   vhost: String = "/",
                   username: String = "guest",
                   password: String = "guest"
                    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStreamFromAQueue(jssc.ssc, rabbitMQHost, rabbitMQQueueName, storageLevel, vhost, username, password)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param ssc              StreamingContext object
   * @param rabbitMQHost     RabbitMQ host string in the format of <host:port>,<host:port>,...
   * @param exchangeName     Exchange name to subscribe to
   * @param routingKeys      Routing keys to subscribe to
   * @param storageLevel     RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStreamFromRoutingKeys(ssc: StreamingContext,
                   rabbitMQHost: String,
                   exchangeName: String,
                   routingKeys: Seq[String],
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
                   vhost: String = "/",
                   username: String = "guest",
                   password: String = "guest"
                    ): ReceiverInputDStream[String] = {
    new RabbitMQInputDStream(
      ssc,
      None,
      rabbitMQHost,
      Some(exchangeName),
      routingKeys,
      storageLevel,
      vhost,
      username,
      password)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param jssc             JavaStreamingContext object
   * @param rabbitMQHost     RabbitMQ host string in the format of <host:port>,<host:port>,...
   * @param exchangeName     Exchange name to subscribe to
   * @param routingKeys      Routing keys to subscribe to
   * @param storageLevel     RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createJavaStreamFromRoutingKeys(jssc: JavaStreamingContext,
                                  rabbitMQHost: String,
                                  exchangeName: String,
                                  routingKeys: java.util.List[String],
                                  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
                                  vhost: String = "/",
                                  username: String = "guest",
                                  password: String = "guest"
                                   ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStreamFromRoutingKeys(jssc.ssc, rabbitMQHost, exchangeName, scala.collection.JavaConversions
      .asScalaBuffer(routingKeys), storageLevel, vhost, username, password)
  }
}
