package com.hongkuncc.demo01_bounded

package object sample03_srctarget_diff2 {
  def main(args: Array[String]): Unit = {
    //步骤
    //1、执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2、计算、显示、或者是保存结果
    //    导入单例类scala中的隐式成员
    import org.apache.flink.api.scala._
    //    迭代计算
    env.readTextFile("hdfs://ns1//flink/a_input")
      .flatMap(.split("\\s+"))
    .filter(.nonEmpty)
    .map((_,1))
      .groupBy(0)
      .sum(1)
      .print

  }

}
