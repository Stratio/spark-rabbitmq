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
package org.apache.spark.streaming.rabbitmq;

import com.rabbitmq.client.QueueingConsumer.Delivery;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.rabbitmq.distributed.JavaRabbitMQDistributedKey;
import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


public final class JavaRabbitMQDistributedConsumer implements Serializable {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("JavaRabbitMQConsumer").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
        java.util.Map<String, String> params = new HashMap<String, String>();
        List<JavaRabbitMQDistributedKey> distributedKeys = new LinkedList<JavaRabbitMQDistributedKey>();

        params.put("hosts", "localhost");
        params.put("vHost", "/");
        params.put("userName", "guest");
        params.put("password", "guest");

        distributedKeys.add(new JavaRabbitMQDistributedKey("rabbitmq-queue",
                new ExchangeAndRouting("rabbitmq-exchange"),
                params
        ));

        Function<Delivery, String> messageHandler = new Function<Delivery, String>() {

            public String call(Delivery message) {
                return new String(message.getBody());
            }
        };

        JavaInputDStream<String> messages =
                RabbitMQUtils.createJavaDistributedStream(jssc, String.class, distributedKeys, params, messageHandler);

        messages.print();

        jssc.start();
        jssc.awaitTermination();
    }
}