package com.hongkuncc.demo02_unbounded.demo03_source

import java.util.Properties

import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.api.scala._
package object ReadDataFromKafkaDemo extends App {
  //1、环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  2、从kafka中获得数据，计算，并显示

  val topic = "raytek"
  val valueDeserializer:DeserializationSchema[String]= new SimpleStringSchema
  val props:Properties = new Properties
  props.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

  env.addSource(new FlinkKafkaConsumer09[String](topic,valueDeserializer,props))
    .print("kafka->")

  env.execute(this.getClass.getSimpleName)
}
