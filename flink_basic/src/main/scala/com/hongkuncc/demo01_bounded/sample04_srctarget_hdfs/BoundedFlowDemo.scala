package com.hongkuncc.demo01_bounded.sample04_srctarget_hdfs

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

object BoundedFlowDemo {
  def main(args: Array[String]): Unit = {
    //前提
    //1、拦截非法的参数
    if(args == null ||args.length!=4){
      println(
        """
          |警告！请录入参数！--input<源的path> -- output<目的地的path>
          |
        """.stripMargin)
      sys.exit(-1)
    }
    //2、获得待计算的源的path,以及计算后的目的地的path
    val tool=ParameterTool.fromArgs(args)
    val inputPath = tool.get("input")
    val outputPath = tool.get("output")


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
      .writeAsText(outputPath,FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)//设置最后一个算子writeAsText的并行度
    //3、正式执行
    env.execute(this.getClass.getSimpleName)

  }


}
