package com.hongkuncc.demo01_bounded.sample01_srctarget_local

import org.apache.flink.api.scala.ExecutionEnvironment



/*
*
* 计算指定源目录下所有文件中单词出现的次数
* */
object BoundedFlowDemo {
  def main(args: Array[String]): Unit = {
    //步骤
    //1、执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2、计算、显示、或者是保存结果
//    导入单例类scala中的隐式成员
    import org.apache.flink.api.scala._
//    迭代计算
    env.readTextFile("C:\\Users\\hongk\\Desktop\\flink\\flink_basic\\a_input\\hello.txt")//要用相对路径
      .flatMap(line=>line.split("\\s+"))
      .filter(line=>line.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print
  }

}
