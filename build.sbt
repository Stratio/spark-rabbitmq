name := "qstream"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.4" % "provided",
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.2" % "provided",
  "com.softwaremill.sttp" %% "core" % "1.3.8",
  "com.softwaremill.sttp" %% "circe" % "1.3.8",
  "com.typesafe.akka" %% "akka-actor" % "2.5.18",
  "com.rabbitmq" % "amqp-client" % "3.6.6")