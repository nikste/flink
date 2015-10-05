/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.scala

import java.io._
import java.lang

import org.apache.flink.api.common.functions.{FlatMapFunction, RichReduceFunction, ReduceFunction, RichGroupReduceFunction}
import org.apache.flink.ml.feature_extraction.{VocabWord, Word2Vec}
import breeze.linalg._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by nikste on 7/6/15.
 */
object testWord2VecNew {
  def main(args: Array[String]): Unit = {

    println("start")


    val env = ExecutionEnvironment.getExecutionEnvironment

    env.getConfig.disableSysoutLogging()

    /*
    var filenames = Array("1000.txt","10000.txt","100000.txt","1000000.txt")
    for (fn <- filenames) {
      var inputData: DataSet[String] = env.readTextFile("/home/nikste/Downloads/enwiki-20141106-pages-articles26.xml-p026625004p029624976/enwiki_res_"+fn)//1000.txt") //("/home/nikste/Downloads/t4_small_small_small_small_newlines")//"/home/nikste/Downloads/t4_stupid")
      //var inputData : DataSet[String] = env.readTextFile("/home/owner/workspace-flink/data/wikipedia/enwiki_res_100000.txt")//("/home/nikste/Downloads/t4_small_small_small_small_newlines")//"/home/nikste/Downloads/t4_stupid")


      //var inputDataArray : DataSet[Array[String]] = inputData.map(article => article.split("\\."))

      //var inputDataSeq : DataSet[Array[String]]  = inputData.map(line => line.split(" "))

      var inputDataSeq = inputData.map(line => line.split("\\."))

      inputDataSeq = inputDataSeq.flatMap[Array[String]] {
        new FlatMapFunction[Array[String], Array[String]] {
          override def flatMap(value: Array[String], out: Collector[Array[String]]): Unit = {

            var stopwordlist = Array("a", "the", "of", "in", "and", "to", "was", "is", "for", "on", "as", "by", "with", "that", "at", "from", "he", "it", "this", "are", "an", "his", "be", "were", "has", "i", "or", "not", "which", "also", "but", "they", "their", "have", "you", "after", "when")

            for (i <- 0 to value.length - 1) {
              var sentence: String = value(i)
              var wordArray: Array[String] = sentence.split(" ").filter(_ != " ").filter(_.nonEmpty).map(_.replaceAll("[,]", "")).map(_.replaceAll("[:]", "")).map(_.replaceAll("[/]", "")).map(_.replaceAll("[']", "")).map(_.replaceAll("[\"]", "")).map(_.toLowerCase) //.filter(!stopwordlist.contains(_))
              out.collect(wordArray)
            }
          }
        }
      }
      var coll = inputDataSeq.collect()
      //var f = scala.tools.nsc.io.File("/home/nikste/workspace-flink/datasets/enwiki-20141106-pages-articles26-100000.txt")
      val fw = new FileWriter("/home/nikste/workspace-flink/datasets/enwiki-20141106-pages-articles26-" + fn,true)//"/home/nikste/workspace-flink/datasets/enwiki-20141106-pages-articles26-1000.txt", true)

      try {
        for (el <- coll) {
          for (el2 <- el) {
            fw.write(el2 + " ")
          }
          fw.write("\n")
        }

      }
      finally fw.close()
    }
*/


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
    /*
   var res = Iterator.tabulate(100){
     index =>
       if(index % 10 == 0){
         Some((index,"yeah"))
       }else{
         None
       }
   }
   var resAr = res.toArray
   println("res:" + res)
   println("number of training data:" + inputDataSeq.count)
*/

    // var inputDataSeq = env.readTextFile("/home/nikste/workspace-flink/datasets/enwiki-20141106-pages-articles26-10000.txt").map(line => line.split(" "))
    //  inputDataSeq = inputDataSeq.filter(_.length > 1)
    /* var dmbefore = breeze.linalg.DenseMatrix((1.0,2.0),(3.0,4.0))
  val dm = breeze.linalg.csvwrite(new File("/home/nikste/workspace-flink/datasets/matrix"),dmbefore,separator=';')

    println("densematrix before")
    println(dmbefore)
  val dmafter = breeze.linalg.csvread(new File("/home/nikste/workspace-flink/datasets/matrix"),separator=';')
    println("densematrix after")
    println(dmafter)



    var hashmap = mutable.HashMap.empty[String, breeze.linalg.DenseVector[Double]]

    hashmap += "test1" -> breeze.linalg.DenseVector(1.0,1.0,1.0)
    hashmap += "test2" -> breeze.linalg.DenseVector(2.0,2.0,2.0)

    println("before")
    for(key <- hashmap){
      println(key)
    }

    var f = new File("/home/nikste/workspace-flink/datasets/hashmap")
    var fos = new FileOutputStream(f)
    var oos = new ObjectOutputStream(fos)
    oos.writeObject(hashmap)
    oos.close


    var fis = new FileInputStream(f)
    var ois = new ObjectInputStream(fis)
    var hashmapRead : mutable.HashMap[String, breeze.linalg.DenseVector[Double]]= ois.readObject().asInstanceOf[ mutable.HashMap[String, breeze.linalg.DenseVector[Double]]]
    ois.close()

    println("after")
    for(key <- hashmapRead){
      println(key)
    }
*/
    /*
   var m = DenseVector(1.0,2.0,2.0)
   println(m.norm(2))
   //println(norm(m,breeze.linalg.Axis._0,2))

    */
    var inputDataSeq = env.readTextFile("/media/nikste/moarspace/workspace-flink/datasets/text8").map(line => line.split(" "))
    val w2v = Word2Vec()


    w2v.fit(inputDataSeq)
    var vocab = Word2Vec.getVocab()

    /*
for(testnumbers <- 0 to 10000){
  println("testnumber:" + testnumbers)
    //var inputDataSeq = env.readTextFile("/media/nikste/moarspace/workspace-flink/datasets/text8").map(line => line.split(" "))
    var in : Array[Array[String]] = new Array[Array[String]](1000)
    for(i <- 0 to in.length - 1 by 10){
      in(i) = Array[String]("b", "a", "b", "a")
      in(i+1) = Array[String]("c", "d", "c", "d")
      in(i+2) = Array[String]("b", "a", "b", "a")
      in(i+3) = Array[String]("c", "d", "c", "d")
      in(i+4) = Array[String]("b", "a", "b", "a")
      in(i+5) = Array[String]("c", "d", "c", "d")
      in(i+6) = Array[String]("a", "a", "a")
      in(i+7) = Array[String]("a", "b")
      in(i+8) = Array[String]("c", "c")
      in(i+9) = Array[String]("c", "d", "d")
    }
    var inputDataSeq = env.fromCollection[Array[String]](in)

    val w2v = Word2Vec()


    w2v.fit(inputDataSeq)
    var vocab = Word2Vec.getVocab()


    // checking vocabulary
    var gt: Array[VocabWord] = Array[VocabWord](
      VocabWord("g", 0, new Array[Int](40), new Array[Int](40), -1, -1),
      VocabWord("g", 0, new Array[Int](40), new Array[Int](40), -1, -1),
      VocabWord("g", 0, new Array[Int](40), new Array[Int](40), -1, -1),
      VocabWord("g", 0, new Array[Int](40), new Array[Int](40), -1, -1)
    )
    gt(0).word = "a"
    gt(0).cn = 1000
    gt(0).point(0) = 2
    gt(0).point(1) = 1
    gt(0).point(2) = -4
    gt(0).code(0) = 1
    gt(0).code(1) = 1
    gt(0).codeLen = 2
    gt(0).ind = 0

    gt(1).word = "c"
    gt(1).cn = 900
    gt(1).point(0) = 2
    gt(1).point(1) = 1
    gt(1).point(2) = -3
    gt(1).code(0) = 1
    gt(1).code(1) = 0
    gt(1).codeLen = 2
    gt(1).ind = 1

    gt(2).word = "d"
    gt(2).cn = 800
    gt(2).point(0) = 2
    gt(2).point(1) = 0
    gt(2).point(2) = -2
    gt(2).code(0) = 0
    gt(2).code(1) = 1
    gt(2).codeLen = 2
    gt(2).ind = 2

    gt(3).word = "b"
    gt(3).cn = 700
    gt(3).point(0) = 2
    gt(3).point(1) = 0
    gt(3).point(2) = -1
    gt(3).code(0) = 0
    gt(3).code(1) = 0
    gt(3).codeLen = 2
    gt(3).ind = 3

    for (si <- 0 to vocab.size - 1) {
      assert(vocab(si).word == gt(si).word)
      assert(vocab(si).cn == gt(si).cn)
      assert(vocab(si).point.sameElements(gt(si).point))
      assert(vocab(si).code.sameElements(gt(si).code))
      assert(vocab(si).codeLen == gt(si).codeLen)
      assert(vocab(si).ind == gt(si).ind)
    }

    var most_sim_a = Word2Vec
    println("end")
  }*/
}
}
