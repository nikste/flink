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

package org.apache.flink.ml.feature_extraction

import breeze.linalg.{DenseVector, DenseMatrix}

/**
 * Created by nikste on 7/1/15.
 */
object nntest {

  val r = scala.util.Random
  def generateTrainingSample() : (DenseVector[Double],DenseVector[Double]) = {
    var ia : Array[Double] = new Array[Double](2)
    for(i <- 0 to ia.size - 1){
      ia(i) = r.nextInt(2)
    }
    var oa = new Array[Double](1)
    if(ia(0) == ia(1)){
      oa(0) = 0
    }else{
      oa(0) = 1
    }
    
    (new DenseVector[Double](ia),new DenseVector[Double](oa))
  
   }
  
  
  def main(args: Array[String]): Unit = {
    println("hei")
    var maxIt = 10000000
    
    var weights = new Array[DenseMatrix[Double]](2)
    weights(0) = DenseMatrix.rand[Double](5,2)
    weights(1) = DenseMatrix.rand[Double](1,5)
    var nn = new NeuralNet( weights,0.1)
    
    var count = 0
    var err = DenseVector[Double](1)
    err(0) = 0
    for(i <- 0 to maxIt - 1){
      var (input,target) = generateTrainingSample()
      err = nn.train(input,target)
      count += 1
      if(count %10000 == 0){
        println(err)
        count = 0
        err(0) = 0
      }
    }
    nn.printweights()
    println("done")
  }
}
