/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.rabbitmq.models

case class ExchangeAndRouting(
                               exchangeName: Option[String] = None,
                               exchangeType: Option[String] = None,
                               routingKeys: Option[String] = None
                             ) {

  def this(exchangeName: String, exchangeType: String, routingKeys: String) =
    this(Option(exchangeName), Option(exchangeType), Option(routingKeys))

  def this(exchangeName: String, routingKeys: String) = this(Option(exchangeName), None, Option(routingKeys))

  def this(exchangeName: String) = this(Option(exchangeName), None, None)

  override def toString(): String =
    s"${exchangeName.getOrElse("")},${exchangeType.getOrElse("")},${routingKeys.getOrElse("")}"

  def toStringPretty(): String =
    s"exchange: ${exchangeName.getOrElse("")}, exchangeType: " +
      s"${exchangeType.getOrElse("")} , routingKeys: ${routingKeys.getOrElse("")}"
}
