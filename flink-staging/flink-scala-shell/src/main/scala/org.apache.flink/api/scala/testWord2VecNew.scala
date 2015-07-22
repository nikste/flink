package org.apache.flink.api.scala

import java.lang

import org.apache.flink.api.common.functions.{FlatMapFunction, RichReduceFunction, ReduceFunction, RichGroupReduceFunction}
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
    
    //var inputData : DataSet[String] = env.readTextFile("/home/nikste/Downloads/enwiki-20141106-pages-articles26.xml-p026625004p029624976/enwiki_res_10000.txt")//("/home/nikste/Downloads/t4_small_small_small_small_newlines")//"/home/nikste/Downloads/t4_stupid")
    var inputData : DataSet[String] = env.readTextFile("/home/owner/workspace-flink/data/wikipedia/enwiki_res_100000.txt")//("/home/nikste/Downloads/t4_small_small_small_small_newlines")//"/home/nikste/Downloads/t4_stupid")



    //var inputDataArray : DataSet[Array[String]] = inputData.map(article => article.split("\\."))

    //var inputDataSeq : DataSet[Array[String]]  = inputData.map(line => line.split(" "))
    var inputDataSeq : DataSet[Array[String]]  = inputData.map(line => line.split("\\."))

    inputDataSeq = inputDataSeq.flatMap[Array[String]]{
      new FlatMapFunction[Array[String],Array[String]]{
        override def flatMap(value: Array[String], out: Collector[Array[String]]): Unit = {



          for(i <- 0 to value.length - 1){
            var sentence : String = value(i)
            var wordArray : Array[String] = sentence.split(" ")
            out.collect(wordArray)
          }
        }
      }
    }

    inputDataSeq = inputDataSeq.filter(_.length > 1)

    /*

    var inputCollected = inputDataSeq.collect()
    var it = inputCollected.iterator
    while(it.hasNext){
      var el = it.next()
      println("-------------------")
      for(i <- 0 to el.size - 1){
        println(el(i))
      }
    }
    */
    println("number of training data:" + inputDataSeq.count)
    val w2v = Word2Vec()
    w2v.fit(inputDataSeq)
    println("end")
  }
}
