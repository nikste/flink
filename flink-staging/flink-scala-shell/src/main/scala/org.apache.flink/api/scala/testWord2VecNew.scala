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
    
    var inputData : DataSet[String] = env.readTextFile("/home/nikste/Downloads/t4_small_small_small_small_small_newlines")

    //println(inputData.count)

    //inputData = inputData.flatMap(_.split("\\.")).map(_.replaceAll("\\s+"," ")).filter(_.length > 20)//.flatMap(_.split(" "))
    var inputDataSeq : DataSet[Array[String]]  = inputData.map(line => line.split(" "))

    
    //var res: DataSet[Int] = testData.map(x => x).reduceGroup[(Int,Int),Int]((x1,x2) => x1._1)

    
    var vecSize = 3
    var vocabSize = 10
    //var m = normalize(DenseMatrix.rand[Double](vecSize,vocabSize), Axis._1,2)
    //println(m)
    
    // cosine similarity
    /*
    var china = normalize(DenseVector[Double](1,1),2)
    var layer0_notNormalized = breeze.linalg.DenseMatrix((1.0,1.0),(-1.0,-1.0),(1.0,-1.0))
    println("not normalized\n" + layer0_notNormalized)
    var layer0 = normalize(layer0_notNormalized,Axis._1,2)
    //println(0.25 * 0.25 + 0.75 * 0.75)
    println(china)
    println(layer0)
    println(  layer0 * china  )*/
   //println(  m * w)
   // var sim =  v*m.t
   // println(sim)
    
    /*var inputWeights = DenseMatrix.rand[Double](vecSize,vocabSize)
    var outputWeights = DenseMatrix.rand[Double](vocabSize,vecSize)
    var input = DenseVector.rand[Double](vocabSize)
    println( outputWeights * (inputWeights * input))*/
    //inputData.first(1).print

    val w2v = Word2Vec()
    w2v.fit(inputDataSeq)

  
    /*
    var weights : DataSet[(breeze.linalg.DenseMatrix[Double],breeze.linalg.DenseMatrix[Double])] = env.fromElements(new Tuple2(breeze.linalg.DenseMatrix.rand[Double](10,10),breeze.linalg.DenseMatrix.rand[Double](10,10)))
    
    var sentences_withkeys : DataSet[(Int,Array[Int])] = env.fromElements((0, Array(1,2,3)),(1, Array(4,5,6,1)))
    
    
    var maxIterations : Int = 1
    //var iterativeOperator = weights.iterate(maxIterations)
    val finalWeights: DataSet[(breeze.linalg.DenseMatrix[Double], breeze.linalg.DenseMatrix[Double])] = weights.iterate(maxIterations)
    {
      previousWeights : DataSet[(breeze.linalg.DenseMatrix[Double], breeze.linalg.DenseMatrix[Double])] => {
        val nextWeights  : DataSet[(DenseMatrix[Double], DenseMatrix[Double])] = sentences_withkeys.reduceGroup {
          // comput updates of weight matrices per "class" / training data partition
          new RichGroupReduceFunction[(Int, Array[Int]), (breeze.linalg.DenseMatrix[Double], breeze.linalg.DenseMatrix[Double],Int)]{
            override def reduce(values: lang.Iterable[(Int, Array[Int])], out: Collector[(DenseMatrix[Double], DenseMatrix[Double], Int)]): Unit = {
              var m1 = DenseMatrix.rand[Double](10,10)
              var m2 = DenseMatrix.rand[Double](10,10)
              out.collect((m1,m2,10))
            }

          }
        }.reduce(new RichReduceFunction[(breeze.linalg.DenseMatrix[Double], breeze.linalg.DenseMatrix[Double],Int),(breeze.linalg.DenseMatrix[Double], breeze.linalg.DenseMatrix[Double])] {
          override def reduce(value1: (breeze.linalg.DenseMatrix[Double], breeze.linalg.DenseMatrix[Double],Int), value2: (breeze.linalg.DenseMatrix[Double], breeze.linalg.DenseMatrix[Double],Int)): (breeze.linalg.DenseMatrix[Double], breeze.linalg.DenseMatrix[Double]) = {
            var res1 = value1._1 :+ value2._1
            var res2 = value1._2 :+ value2._2

            //var totalCount : Double= value1._3 + value2._3
            //res1 :*= value1._3.toDouble / totalCount
            //res2 :*= value2._3.toDouble / totalCount

            (res1,res2)//,0)
          }
        }).name("holger")
        nextWeights
      }
    }*/
    
    
    
    println("end")
  }
}
