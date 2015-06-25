/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.scala

/**
 * Created by nikste on 5/28/15.
 */

import java.lang.Iterable
import java.util

import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.IterativeDataSet
import org.apache.flink.ml.feature_extraction.Word2vec
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object test {
  def main(args: Array[String]) {
    println("start")
  
    val env = ExecutionEnvironment.getExecutionEnvironment

    /*
    var sentences : DataSet[String] = env.fromElements("a","b","c","d","e","f","g","h","i","j","k","l", "m", "n", "o", "p", "q", "r" ,"s" ,"t" ,"u" ,"v", "w" ,"x" ,"y" ,"z")

    // additional parameter batchsize = 
    var batchsize = 2
    
    var sentencecount : Long = sentences.count
    
    // number of keys = sentencecounts / batchsize
    var num_keys : Long = sentencecount / batchsize

    println("num_keys:" + num_keys)
    
    
    var max = num_keys
    
    // (w1,w2)
    var weights = env.fromElements((1,2))
    // touple (key,sentence)
    var sentences_withkeys :GroupedDataSet[(Int,String)] = sentences.map(new keyGeneratorFunction(max.toInt)).name("mapSentences").groupBy(0)
    
    
    var maxIterations : Int = 2
    //var iterativeOperator = weights.iterate(maxIterations)
    val finalWeights: DataSet[(Int, Int)] = weights.iterate(maxIterations)
    {
      previousWeights => {
        val nextWeights = sentences_withkeys.reduceGroup {
          // comput updates of weight matrices per "class" / training data partition
          new RichGroupReduceFunction[(Int, String), (Int, Int)] {
            override def reduce(values: Iterable[(Int, String)], out: Collector[(Int, Int)]): Unit = {
              var it = values.iterator()
              var iterativeWeights: (Int, Int) = getIterationRuntimeContext.getBroadcastVariable[(Int, Int)]("iterativeWeights").get(0)
              var w1 = iterativeWeights._1
              var w2 = iterativeWeights._2

              var sum_w1 = w1
              var sum_w2 = w2

              var counter = 0
              while (it.hasNext) {
                var ne = it.next()
                sum_w1 += ne._2.size
                sum_w2 += ne._2.size
                counter += 1
              }
              println(sum_w1, sum_w2, counter)
              out.collect((sum_w1, sum_w2))
            }
          }
        }.name("reduceGroup->sentences_withKeys").withBroadcastSet(previousWeights, "iterativeWeights")
          .reduce(new ReduceFunction[(Int, Int)] {
          override def reduce(value1: (Int, Int), value2: (Int, Int)): (Int, Int) = {
            (value1._1 + value2._1, value1._2 + value2._2)
          }
        }).name("holger")
        nextWeights
      }
    }
    finalWeights.print("test")
    println(finalWeights.getExecutionEnvironment.getExecutionPlan())
    
    */
    
    
    
    
    println("end")
    
    var inputData : DataSet[String] = env.readTextFile("/home/nikste/Downloads/t4_small_small_small_small")
    
    println(inputData.count)
    
    inputData = inputData.flatMap(_.split("\\.")).map(_.replaceAll("\\s+"," ")).filter(_.length > 20)//.flatMap(_.split(" "))
    
    env.getConfig.disableSysoutLogging()
    
    val w2v = Word2vec()
    w2v.fit(inputData)
    
  }
}
