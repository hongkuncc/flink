package com.hongkuncc.demo01_bounded.sample01_srctarget_local


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
    env.readTextFile("a_input/hello.txt")
      .flatMap(.split(\\s+))
      .filter(.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print
  }

}
