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
