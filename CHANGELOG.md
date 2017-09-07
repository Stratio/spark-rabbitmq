# Changelog

## 0.6.0 (upcoming)

* Pending changelog

## 0.5.0 (December 15, 2016)

* Upgrade to Spark 2.0
* Optimized accumulators with AccumulatorV2 API
* Removed crossbuild with Spark 1.6 and 1.5

## 0.4.0 (December 15, 2016)

* Upgrade scala version to 2.11.8
* Upgrade to rabbitMQ library version 3.6.6
* Removed Array[Byte] in createStream API
* The library now run with Delivery message of RabbitMQ
* Added tests with Delivery message option
* Bugfix: Send noAck when store messages fails
* Bugfix: Integer conversion in message parameters
* Bugfix: Close connections correctly

## 0.3.0 (June 2016)

* Compatibility for Spark version 1.6.1
* Bug fixing
* Distributed RabbitMQ consumer with direct approach
* Parallelize the consumption in one node, to have more than one consumer
* Encapsulated consumer and pool of consumers
* Optimized RabbitMQRDD functions in order to have better performance
* RabbitMQ messages can be typed
* Integration tests added for Scala implementation
* Java examples and refactor rabbitMQUtils

## 0.2.0 (January 2016)

* Compatibility for Spark versions 1.4 and 1.5
* Bug fixing

## 0.1.0 (sometime)

* First version
