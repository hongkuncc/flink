package com.hongkuncc.demo01_bounded


import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

package object BoundedFlowDemo {
  def main(args: Array[String]): Unit = {
    //步骤
    //1、执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2、计算、显示、或者是保存结果
    //    导入单例类scala中的隐式成员
    import org.apache.flink.api.scala._
    //    迭代计算
    env.readTextFile("a_input")
      .flatMap(line=>line.split("\\s+"))
      .filter(line=>line.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .writeAsText("hdfs://ns1//flink/a_input",FileSystem.WriteMode.OVERWRITE)

    //3、正式执行
    env.execute(this.getClass.getSimpleName)
  }

}
