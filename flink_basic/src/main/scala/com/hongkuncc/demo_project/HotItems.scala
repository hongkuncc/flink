package com.hongkuncc.demo_project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // 创建一个 StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
    env.setParallelism(1)
    val stream = env
      // 以window下为例，需替换成自己的路径
      .readTextFile("YOUR_PATH\\resources\\UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
      // 指定时间戳和watermark
      .assignAscendingTimestamps(_.timestamp * 1000)

    env.execute("Hot Items Job")
  }
}

