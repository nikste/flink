package org.apache.flink.api.scala

import org.apache.flink.ml.feature_extraction.Word2Vec

/**
 * Created by nikste on 7/6/15.
 */
object testWord2VecNew {
  def main(args: Array[String]): Unit = {

    println("start")
    
    
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.getConfig.disableSysoutLogging()
    
    var inputData : DataSet[String] = env.readTextFile("/home/nikste/Downloads/t4_small_small_small_small_newlines")

    //println(inputData.count)

    //inputData = inputData.flatMap(_.split("\\.")).map(_.replaceAll("\\s+"," ")).filter(_.length > 20)//.flatMap(_.split(" "))
    var inputDataSeq : DataSet[Array[String]]  = inputData.map(line => line.split(" "))

    //inputData.first(1).print

    val w2v = Word2Vec()
    w2v.fit(inputDataSeq)
    
    println("end")
  }
}
