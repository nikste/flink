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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.feature_extraction.Word2vec

object test {
  def main(args: Array[String]) {
  println("start")
  
    val env = ExecutionEnvironment.getExecutionEnvironment


/*    val rawLines : DataSet[String] = env.readTextFile("file:///home/nikste/workspace-flink/enwik9_clean");
    // convert to sentences
    val rawText : DataSet[String] = rawLines.reduce((w1,w2) => w1 + w2)
    val linesDS = rawLines.reduce((w1,w2) => w1 + w2)
      .flatMap {
      _.toLowerCase.split("\\.") filter {
      _.nonEmpty
    }
  }
//    inputData.print()
  */
  /*  val inputData = env.fromElements(
  "a a a a a a a a a a a a a a a a a", 
  "b b b b b b b b b b b b b b b b",
  "c c c c c c c c c c c c c c c",
  "d d d d d d d d d d d d d d",
  "e e e e e e e e e e e e e",
  "f f f f f f f f f f f f",
  "g g g g g g g g g g g ",
  "h h h h h h h h h h",
  "i i i i i i i i i",
  "j j j j j j j j",
  "k k k k k k k",
  "l l l l l l",
  "m m m m m")*/
    
    //env.getConfig.disableSysoutLogging()
    var inputData : DataSet[String] = env.readTextFile("/home/nikste/Downloads/t4_small_small_small_small")
    //var inputData = env.fromElements("I go home","I go home","I go kitchen","I go kitchen","I home my")
    //env.getConfig.disableSysoutLogging() 
    
    //var inputData = env.fromElements("A word in a sentence . . i.e. fucking stupid")
    //inputData = inputData.map(_.split(' '))
    println(inputData.count)
    //println("before::::")
    //inputData.print
    /*
    inputData = inputData.reduce((a,b) => a +" " +b)
    inputData = inputData.flatMap(_.split("\\.")).flatMap(_.split(' '))
    inputData = inputData.filter(_.length > 5)
    */
    inputData = inputData.flatMap(_.split("\\.")).map(_.replaceAll("\\s+"," ")).filter(_.length > 20)//.flatMap(_.split(" "))
    
    println("after::::")
    //inputData.print
    env.getConfig.disableSysoutLogging()
    
    val w2v = Word2vec()
    w2v.fit(inputData)
    
    
    
/*
     inputData = inputData.filter(_.length > 20)
   println(inputData.count)
  
    env.getConfig.disableSysoutLogging()
    //inputData = inputData.filter(_.nonEmpty).filter(_.length > 2)

    //inputData.print
    /*
    println(inputData.count)
    //inputData = inputData.map(_.filterNot(_.forall(_.isEmpty)))
    inputData = inputData.filter(_.length > 5)
    println(inputData.count)
    
    
    inputData.print()
    */
    //println("inputdatacoutn" + inputData.count()) 
    //inputData.print()
    
    
    val w2v = Word2vec()
    w2v.fit(inputData)
  
  //inputData.print()
    //println("inputData.coutn()="+rawText.print())
    //println("rawLines.count() = ")
    //println(linesDS.print())
    println("end")
*/
}
}
