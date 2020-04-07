package com.hongkuncc.demo01_bounded.sample02_srctarget_diff
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem


object BoundedFlowDemo {
  def main(args: Array[String]): Unit = {
    //步骤
    //1、执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2、计算、显示、或者是保存结果

    //设置所有算子的全局的并行度
    env.setParallelism(1)

    //    导入单例类scala中的隐式成员
    import org.apache.flink.api.scala._
    //    迭代计算
    env.readTextFile("hdfs://ns1//flink/a_input")
      .flatMap(line=>line.split("\\s+"))
      .filter(line=>line.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .writeAsText("hdfs://",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)//设置最后一个算子的并行度
    //3、正式执行
    env.execute(this.getClass.getSimpleName)

  }

}
