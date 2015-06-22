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

import java.util

import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.IterativeDataSet
import org.apache.flink.ml.feature_extraction.Word2vec
import org.apache.flink.util.Collector

import scala.util.Random

object test {
  def main(args: Array[String]) {
    println("start")
  
    val env = ExecutionEnvironment.getExecutionEnvironment


    var sentences : DataSet[String] = env.fromElements("a","b","c","d","e","f")

    // additional parameter batchsize = 
    var batchsize = 2
    
    var sentencecount : Long = sentences.count
    
    // number of keys = sentencecounts / batchsize
    var num_keys : Long = sentencecount / batchsize


    
    // generates number between min (inclusive) and max(exclusive)
    /*def generateKey(min : Int,max : Int) : Int = {
      var R = r.nextInt(max-min) + max;
      R
    }*/
    var max = num_keys
    var weightmatrices = env.fromElements((1,2))
    
    var sentences_withkeys = sentences.map(new keyGeneratorFunction(max.toInt))//{ x => ( generateKey(0, num_keys.toInt), x )}
    var weights_withkeys = weightmatrices.flatMap{new FlatMapFunction[(Int,Int),(Int,Int,Int)]{
      def flatMap(input:(Int,Int),output:Collector[(Int,Int,Int)]): Unit ={
          for(i <- 0 to num_keys.toInt){
            output.collect((i,input._1,input._2))
          }
        }
      }
    }
    
    sentences_withkeys.print
    weights_withkeys.print
    var joint = sentences_withkeys.join(weights_withkeys).where(0).equalTo(0)
    joint.print
    
    
    /*
    val count = test.iterate(1000000) { iterationInput: DataSet[Int] =>
      val result = iterationInput.map { i =>
        val x = Math.random()
        val y = Math.random()
        i + (if (x * x + y * y < 1) 1 else 0)
      }
      result
    }
    
    val result = count.map{ c => c /10000.0 * 4 }
    result.print()
    */
    
    
    println("end")
    /*
    var inputData : DataSet[String] = env.readTextFile("/home/nikste/Downloads/t4_small_small_small_small")
    
    println(inputData.count)
    
    inputData = inputData.flatMap(_.split("\\.")).map(_.replaceAll("\\s+"," ")).filter(_.length > 20)//.flatMap(_.split(" "))
    
    env.getConfig.disableSysoutLogging()
    
    val w2v = Word2vec()
    w2v.fit(inputData)
    */
  }
}
