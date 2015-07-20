package org.apache.flink.api.scala

import java.lang

import org.apache.flink.api.common.functions.{RichReduceFunction, ReduceFunction, RichGroupReduceFunction}
import org.apache.flink.ml.feature_extraction.Word2Vec
import breeze.linalg._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Created by nikste on 7/6/15.
 */
object testWord2VecNew {
  def main(args: Array[String]): Unit = {

    println("start")
    
    
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.getConfig.disableSysoutLogging()
    
    var inputData : DataSet[String] = env.readTextFile("/home/nikste/Downloads/t4_small_small_small_small_newlines")//"/home/nikste/Downloads/t4_stupid")

    var inputDataSeq : DataSet[Array[String]]  = inputData.map(line => line.split(" ")).filter(_.length > 1)

   

    var v1 : DenseVector[Float] = DenseVector[Float](1.0f,2.0f,3.0f)
    var v2 : DenseMatrix[Float] = DenseMatrix.fill[Float](3,3){10.0f}//((10.0f,10.0f,10.0f),(10.0f,10.0f,10.0f),(10.0f,10.0f,10.0f))
    var res : Float = v2(0,::) * v1
    println(res)
    
    val w2v = Word2Vec()
    w2v.fit(inputDataSeq)

    
    
    
    println("end")
  }
}
