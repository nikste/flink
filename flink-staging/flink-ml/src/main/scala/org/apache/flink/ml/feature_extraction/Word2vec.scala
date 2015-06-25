
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
package org.apache.flink.ml.feature_extraction


import java.io.{ObjectInputStream, IOException}
import java.lang.Iterable
import java.text.SimpleDateFormat
import java.util
import java.util.Random

import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.operators.IterativeDataSet
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.feature_extraction.Word2vec._
import org.apache.flink.ml.math._
import org.apache.flink.ml.pipeline.{TransformOperation, FitOperation, Transformer}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuilder}
import scala.reflect.ClassTag





import java.nio.ByteBuffer
import java.util.{Random => JavaRandom, Calendar}

import scala.util.{Random => ScalaRandom}
import scala.util.hashing.MurmurHash3


/**
 * This class implements a XORShift random number generator algorithm
 * Source:
 * Marsaglia, G. (2003). Xorshift RNGs. Journal of Statistical Software, Vol. 8, Issue 14.
 * @see <a href="http://www.jstatsoft.org/v08/i14/paper">Paper</a>
 * This implementation is approximately 3.5 times faster than
 * { java.util.Random java.util.Random}, partly because of the algorithm, but also due
 * to renouncing thread safety. JDK's implementation uses an AtomicLong seed, this class
 * uses a regular Long. We can forgo thread safety since we use a new instance of the RNG
 * for each thread.
 */
private class XORShiftRandom(init: Long) extends JavaRandom(init) {
  
  def this() = this(System.nanoTime)

  private var seed = XORShiftRandom.hashSeed(init)

  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
  override protected def next(bits: Int): Int = {
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    seed = nextSeed
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long) {
    seed = XORShiftRandom.hashSeed(s)
  }
}
class keyGeneratorFunction(max:Int) extends MapFunction[Array[Int], (Int, Array[Int])]{

  var seed = 10
  @transient
  var r : Random = new Random()//seed) //= 
  /*keyGeneratorFunction(max:Int){
    r = new scala.util.Random(seed)
    
  }//= new scala.util.Random(seed);
  */
  @throws(classOf[IOException])
  private def readObject(input:ObjectInputStream) : Unit = {
    input.defaultReadObject()
    r = new Random(seed)
  }

  /*def keyGeneratorFunction(max:Int): Unit ={
    r = new scala.util.Random(seed)
    
  }*/

  override def map(value: Array[Int]): (Int, Array[Int]) = {
    var R = r.nextInt(max);
    (R,value)
  }
}
/** Contains benchmark method and main method to run benchmark of the RNG */
private object XORShiftRandom {

  /** Hash seeds to have 0/1 bits throughout. */
  private def hashSeed(seed: Long): Long = {
    val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
    MurmurHash3.bytesHash(bytes)
  }

  /**
   * Main method for running benchmark
   * @param args takes one argument - the number of random numbers to generate
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Benchmark of XORShiftRandom vis-a-vis java.util.Random")
      println("Usage: XORShiftRandom number_of_random_numbers_to_generate")
      System.exit(1)
    }
    println(benchmark(args(0).toInt))
  }
  /**
   * Method executed for repeating a task for side effects.
   * Unlike a for comprehension, it permits JVM JIT optimization
   */
  def times(numIters: Int)(f: => Unit): Unit = {
    var i = 0
    while (i < numIters) {
      f
      i += 1
    }
  }

  /**
   * Timing method based on iterations that permit JVM JIT optimization.
   * @param numIters number of iterations
   * @param f function to be executed. If prepare is not None, the running time of each call to f
   *          must be an order of magnitude longer than one millisecond for accurate timing.
   * @param prepare function to be executed before each call to f. Its running time doesn't count.
   * @return the total time across all iterations (not couting preparation time)
   */
  def timeIt(numIters: Int)(f: => Unit, prepare: Option[() => Unit] = None): Long = {
    if (prepare.isEmpty) {
      val start = System.currentTimeMillis
      times(numIters)(f)
      System.currentTimeMillis - start
    } else {
      var i = 0
      var sum = 0L
      while (i < numIters) {
        prepare.get.apply()
        val start = System.currentTimeMillis
        f
        sum += System.currentTimeMillis - start
        i += 1
      }
      sum
    }
  }
  /**
   * @param numIters Number of random numbers to generate while running the benchmark
   * @return Map of execution times for { java.util.Random java.util.Random}
   * and XORShift
   */
  def benchmark(numIters: Int): Map[String, Long] = {

    val seed = 1L
    val million = 1e6.toInt
    val javaRand = new JavaRandom(seed)
    val xorRand = new XORShiftRandom(seed)

    // this is just to warm up the JIT - we're not timing anything
    timeIt(million) {
      javaRand.nextInt()
      xorRand.nextInt()
    }

    /* Return results as a map instead of just printing to screen
    in case the user wants to do something with them */
    Map("javaTime" -> timeIt(numIters) { javaRand.nextInt() },
      "xorTime" -> timeIt(numIters) { xorRand.nextInt() })
  }
}
/**
 * Entry in vocabulary 
 */
case class VocabWord(
                              var word: String,
                              var cn: Int,
                              var point: Array[Int],
                              var code: Array[Int],
                              var codeLen: Int,
                              var ind: Int // adding index as in 1-hot encoding
                              ){
  override def toString(): String ={
    var points = ""
    for (i <- 0 to point.length - 1){points += point(i) + ","}
    points += point(point.length - 1)

    var codes = ""
    for (i <- 0 to code.length - 1){codes += code(i) + ","}
    codes += code(code.length - 1)

    val res = "VocabWord(" +
    " word:" + word +
    " cn:" + cn +
    " point:" + points +
    " code:" + codes +
    " codeLen:" + codeLen +
    " index:" + ind + ")"
    res
  }
}
/**
 * :: Experimental ::
 * Word2Vec creates vector representation of words in a text corpus.
 * The algorithm first constructs a vocabulary from the corpus
 * and then learns vector representation of words in the vocabulary. 
 * The vector representation can be used as features in 
 * natural language processing and machine learning algorithms.
 *
 * We used skip-gram model in our implementation and hierarchical softmax 
 * method to train the model. The variable names in the implementation
 * matches the original C implementation.
 *
 * For original C implementation, see https://code.google.com/p/word2vec/ 
 * For research papers, see 
 * Efficient Estimation of Word Representations in Vector Space
 * and 
 * Distributed Representations of Words and Phrases and their Compositionality.
 */

class Word2vec extends Transformer[Word2vec] {

  /*
  private var vectorSize = 100
  private var learningRate = 0.025
  private var numPartitions = 1
  private var numIterations = 1
  private var minCount = 5
  // context words from [-window, window] 
  private val window = 5
  */
  def setVectorSize(vectorSizeValue: Int): Word2vec = {
    parameters.add(VectorSize, vectorSizeValue)
    this
  }

  def setLearningRate(learningRateValue: Double): Word2vec = {
    parameters.add(LearningRate, learningRateValue)
    this
  }

  def setNumIterations(numIterationsValue: Int): Word2vec = {
    parameters.add(NumIterations, numIterationsValue)
    this
  }

  def setMinCount(minCountValue: Int): Word2vec = {
    parameters.add(MinCount, minCountValue)
    this
  }

  def setWindowSize(windowSizeValue: Int): Word2vec = {
    parameters.add(WindowSize, windowSizeValue)
    this
  }

  // sparks internal vars
  private var seed = 1.toLong
  //TODO: change to random //Utils.random.nextLong()
  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6
  private val MAX_CODE_LENGTH = 40
  private val MAX_SENTENCE_LENGTH = 1000


  private var trainWordsCount = 0
  private var vocabSize = 0
  private var vocab: Seq[VocabWord] = null
  private var vocabHash = mutable.HashMap.empty[String, Int]


  // extra
  private var MAX_STRING = 100
  private var vocab_size = 0

}

object Word2vec {

  // ====================================== Parameters =============================================

  case object VectorSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(300)
  }

  case object LearningRate extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.025)
  }

  case object NumIterations extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  case object MinCount extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(5)
  }

  case object WindowSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(5)
  }

  // ==================================== Factory methods ==========================================

  def apply(): Word2vec = {
    new Word2vec()
  }

  // ====================================== Operations =============================================


  //private var vocab : Seq[VocabWord] = null
  private var vocabSize: Int = 0

  private var vocabHash = mutable.HashMap.empty[String, Int]
  private var trainWordsCount = 0

  private val MAX_CODE_LENGTH: Int = 40

  // exp table vars for hierarchical softmax
  private var seed = 1.toLong
  //TODO: chang to random //Utils.random.nextLong()
  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6

  // building sentence huffman representation
  private val MAX_SENTENCE_LENGTH = 1000

  /**
   * builds up Dictionary from input corpus for further processing
   * (one hot representation, words most often used appear first)
   * @param words
   */
  def learnVocab(words: DataSet[String], minCount: Int): (DataSet[VocabWord], DataSet[mutable.HashMap[String, Int]], DataSet[(VocabWord, Int)]) = {

    val vocab: DataSet[VocabWord] = words.flatMap(_.split(" ").filter(!_.isEmpty)) // also filters whitespace (they do not count as words)
      .map {
      (_, 1)
    }
      .groupBy(0).sum(1) //reduceByKey(_+_)
      .map(x => VocabWord(
      x._1,
      x._2,
      new Array[Int](MAX_CODE_LENGTH),
      new Array[Int](MAX_CODE_LENGTH),
      0,
      0))
      .filter(_.cn >= minCount)
      // avoid collect, could be a bottleneck since it runs on local vm and is defaulted to 512 mb.
      .setParallelism(1)
      .sortPartition("cn", Order.DESCENDING)

    val vocabIndices: DataSet[(VocabWord, Int)] = vocab //.map{(_,1)}
      .reduceGroup {
      new GroupReduceFunction[VocabWord, (VocabWord, Int)] {
        override def reduce(values: Iterable[VocabWord], out: Collector[(VocabWord, Int)]): Unit = {
          val it = values.iterator()
          var counter = 0;

          while (it.hasNext) {
            val current = it.next()
            out.collect((current, counter))
            counter += 1
          }
        }
      }
    }.setParallelism(1)


    val vocabHash2 = vocabIndices.reduceGroup {
      new GroupReduceFunction[(VocabWord, Int), mutable.HashMap[String, Int]] {
        override def reduce(values: Iterable[(VocabWord, Int)], out: Collector[mutable.HashMap[String, Int]]): Unit = {

          var outputHash: mutable.HashMap[String, Int] = mutable.HashMap.empty[String, Int]

          outputHash += "string" -> 2
          val it = values.iterator()
          var counter = 0;

          while (it.hasNext) {
            var current: (VocabWord, Int) = it.next();

            outputHash += current._1.word -> current._2
            counter += 1
          }
          out.collect(outputHash)

        }
      }
    }.setParallelism(1)


    // number of distinct words in the corpus
    vocabSize = vocab.count().toInt

    require(vocabSize > 0, "The vocabulary size should be > 0. You may need to check " +
      "the setting of minCount, which could be large enough to remove all your words in sentences.")

    // counts the number of actual words in the corpus
    trainWordsCount = vocab.map(w1 => w1.cn).reduce((n1, n2) => n1 + n2).collect()(0)

    (vocab, vocabHash2, vocabIndices)
  }

  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }

  def createBinaryTree(vocab: DataSet[VocabWord]): DataSet[VocabWord] = {

    // mapping from word index to number of word-usage in corpus
    val count = new Array[Long](vocabSize * 2 + 1)

    val binary = new Array[Int](vocabSize * 2 + 1)
    val parentNode = new Array[Int](vocabSize * 2 + 1)
    val code = new Array[Int](MAX_CODE_LENGTH)
    val point = new Array[Int](MAX_CODE_LENGTH)
    var a = 0

    val vocabCollected = vocab.collect() //TODO: remove?
    var vocabCollectedIt = vocabCollected.iterator
    while (a < vocabSize && vocabCollectedIt.hasNext) {
      val current = vocabCollectedIt.next()
      count(a) = current.cn
      a += 1
    }
    while (a < 2 * vocabSize) {
      count(a) = 1e9.toInt
      a += 1
    }
    var pos1 = vocabSize - 1
    var pos2 = vocabSize

    var min1i = 0
    var min2i = 0

    a = 0
    while (a < vocabSize - 1) {
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min2i = pos1
          pos1 -= 1
        } else {
          min2i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 += 1
      }
      count(vocabSize + a) = count(min1i) + count(min2i)
      parentNode(min1i) = vocabSize + a
      parentNode(min2i) = vocabSize + a
      binary(min2i) = 1
      a += 1
    }
    // Now assign binary code to each vocabulary word
    var i = 0
    a = 0

    vocabCollectedIt = vocabCollected.iterator
    while (a < vocabSize && vocabCollectedIt.hasNext) {
      val current = vocabCollectedIt.next()
      current.ind = a
      var b = a
      i = 0
      while (b != vocabSize * 2 - 2) {
        code(i) = binary(b)
        point(i) = b
        i += 1
        b = parentNode(b)
      }
      current.codeLen = i
      current.point(0) = vocabSize - 2
      //vocab(a).codeLen = i
      //vocab(a).point(0) = vocabSize - 2
      b = 0
      while (b < i) {
        current.code(i - b - 1) = code(b)
        current.point(i - b) = point(b) - vocabSize
        //vocab(a).code(i - b - 1) = code(b)
        //vocab(a).point(i - b) = point(b) - vocabSize
        b += 1
      }
      a += 1
    }

    var vocab2 = vocab.getExecutionEnvironment.fromCollection(vocabCollected)
    vocab2.print()
    vocab2
  }


  def convertSentenceWordsToIndexes(words: DataSet[String], hash: DataSet[mutable.HashMap[String, Int]]): DataSet[Array[Int]] = {

    //split at whitespace and make array
    val sentencesStrings: DataSet[Array[String]] = words.flatMap {
      new FlatMapFunction[String, Array[String]] {
        override def flatMap(value: String, out: Collector[Array[String]]): Unit = {
          out.collect(value.split(" "))
        }
      }
    }

    // convert words inarray to 1-hot encoding
    val sentencesInts: DataSet[Array[Int]] = sentencesStrings.map {
      new RichMapFunction[Array[String], Array[Int]] {

        override def map(value: Array[String]): Array[Int] = {

          //val hash = getRuntimeContext.getBroadcastVariable("bcHash").get(0)
          val hashList: util.List[mutable.HashMap[String, Int]] = getRuntimeContext.getBroadcastVariable[mutable.HashMap[String, Int]]("bcHash")
          val hash: mutable.HashMap[String, Int] = hashList.get(0);

          var list = ListBuffer[Int]()

          for (word <- value) {
            val wordInt = hash.get(word)
            wordInt match {
              case Some(w) => list.append(w)
              case None =>
            }
          }
          list.toArray
        }
      }
    }.withBroadcastSet(hash, "bcHash")

    sentencesInts
  }


  /**
   * initializes neural network to be trained to output word vectors
   */
  def initNetwork(resultingParameters: ParameterMap): (DenseMatrix, DenseMatrix) = {
    //val initRandom = new XORShiftRandom(seed)

    val vectorSizeOpt = resultingParameters.get[Int](VectorSize)
    var vectorSize = 0

    vectorSizeOpt match {
      case Some(i) => vectorSize = i;
      case None => throw new RuntimeException("Vector size invalid!")
    }

    if (vocabSize * vectorSize * 8 >= Int.MaxValue) {
      throw new RuntimeException("Too much information!!! " +
        "Please increase minCount or decrease vectorSize in Word2Vec +" +
        " to avoid an OOM. You are highly recommended to make your vocabSize*vectorSize," +
        "which is " + vocabSize + "*" + VectorSize + " for now, less than `Int.MaxValue/8`.")
    }

    val initRandom = new XORShiftRandom(seed)
    val syn0: DenseMatrix = new DenseMatrix(vectorSize, vocabSize, Array.fill[Double](vocabSize * vectorSize)((initRandom.nextFloat() - 0.5f) / vectorSize))

    val syn1: DenseMatrix = new DenseMatrix(vocabSize, vectorSize, Array.fill[Double](vocabSize * vectorSize)((initRandom.nextFloat() - 0.5f) / vectorSize))
    //val syn0Global : Array[Float] =
    //  Array.fill[Float](vocabSize * vectorSize)((initRandom.nextFloat() - 0.5f) / vectorSize)
    //val syn1Global : Array[Double]= new Array[Double](vocabSize * vectorSize)
    //val syn1 : DenseMatrix = new DenseMatrix(vocabSize,vectorSize,syn1Global)

    // neural network layers
    (syn0, syn1)
  }

  /*
  def dotMapReduce(A: DataSet[(Int,Int,Double)],B: DataSet[(Int,Int,Double)]): DataSet[(Int,Int,Double)] = {
   */

  // for loop
  /*
    for (int i = 0; i < n; i++)
      for (int j = 0; j < n; j++)
        for (int k = 0; k < n; k++)
          c[i][j] = c[i][j] + a[i][k]*b[k][j];
    
    for(i <- 0 to Rows - 1){
      for(j <- 0 to Cols - 1){
        for(k <- 0 to n - 1) {
          res(i, j) += a(i, k) * b(k, j)
        }
      }
    }
  }*/


  def activation_function(in: Double): Double = {
    var out = 1.0 / (1.0 + math.exp(-in))
    out
  }

  /*  
  def train_sg_iterative_matrix(expTable :Array[Float],vocab: Array[VocabWord],layer0:DenseMatrix,layer1:DenseMatrix,inIdx:Int, outIdx:Int,last_it:Boolean):(DenseMatrix,DenseMatrix,Double) = {
    var error = 0.0
    var learningRate = 0.1

    var vectorSize = layer0.numRows

    // input -> hidden
    var hidden_act = DenseVector(Array.fill[Double](vectorSize)(0.0))
    for(i <- 0 to vectorSize - 1){
      hidden_act(i) = layer0(i,inIdx)
    }
    
    // hidden -> output
    var numOutputs = vocab(outIdx).codeLen

    var errgrad1 = DenseMatrix.zeros(vectorSize,numOutputs)
    for(netOutputIdx <- 0 to numOutputs - 1){
    
      var netOutputNum = vocab(outIdx).point(netOutputIdx)
      var target = vocab(outIdx).code(netOutputIdx)

      var output_net = 0.0
      for(i <- 0 to vectorSize - 1){
        output_net += layer1(netOutputNum,i) * hidden_act(i)
      }

      var output_act = output_net
      
      if (output_act > -MAX_EXP && output_act < MAX_EXP) {
        var ind = ((output_act + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
        output_act = expTable(ind)
        var org_output_act = activation_function(output_net)
        println("output_act original = " + org_output_act + " from table:" + output_act)
      }
      //var output_act = activation_function(output_net)

      error += math.abs(output_act - target)

      var output_error_gradient = ( output_act - target) * (1 - output_act ) * output_act


      if(last_it){
        println("gt = " + target + "  is = " + output_act)
      }

      // backpropagation

      // layer1 update

      for(i <- 0 to vectorSize - 1){
        layer1(netOutputNum,i) -= learningRate * output_error_gradient * hidden_act(i)
        errgrad1(i,netOutputIdx) += output_error_gradient * layer1(netOutputNum,i)
      }
    }

    // layer0 update
    for(netOutputIdx <- 0 to numOutputs - 1){
      for(i <- 0 to vectorSize - 1){
        if(hidden_act(i) < 0.000000000001){
          hidden_act(i) = 0.0000000000001
        }
        layer0(i,inIdx) += -learningRate * errgrad1(i,netOutputIdx) * hidden_act(i) * (1 - hidden_act(i))
      }
    }

    (layer0,layer1,error)
  }
*/
  
    def train_sg_test_iterative(vocab: Array[VocabWord],layer0:DenseMatrix,layer1:DenseMatrix,inIdx:Int, outIdx:Int,last_it:Boolean):(DenseMatrix,DenseMatrix,Double) = {

    var error = 0.0
    var learningRate = 0.1

    var vectorSize = layer0.numRows

    // input -> hidden
    var hidden_net = DenseVector(Array.fill[Double](vectorSize)(0.0))
    for(i <- 0 to vectorSize - 1){
      hidden_net(i) = layer0(i,inIdx)
    }
    
    var hidden_act = DenseVector(Array.fill[Double](vectorSize)(0.0))
    for(i <- 0 to vectorSize - 1) {
      hidden_act(i) = hidden_net(i)//activation_function(hidden_net(i))
    }


    
    // hidden -> output
    var numOutputs = vocab(outIdx).codeLen

    var errgrad1 = DenseMatrix.zeros(vectorSize,numOutputs)
    for(netOutputIdx <- 0 to numOutputs - 1){
      var netOutputNum = vocab(outIdx).point(netOutputIdx)
      var target = vocab(outIdx).code(netOutputIdx)

      var output_net = 0.0
      for(i <- 0 to vectorSize - 1){
        output_net += layer1(netOutputNum,i) * hidden_act(i)
      }

      
      
      var output_act = activation_function(output_net)

      error += math.abs(output_act - target)
      
      var output_error_gradient = ( output_act - target) * (1 - output_act ) * output_act

      
      if(last_it){
        println("gt = " + target + "  is = " + output_act)
      }

      // backpropagation

      // layer1 update
      
      for(i <- 0 to vectorSize - 1){
        layer1(netOutputNum,i) -= learningRate * output_error_gradient * hidden_act(i)
        errgrad1(i,netOutputIdx) += output_error_gradient * layer1(netOutputNum,i)
      }
      
    }

    // layer0 update
    for(netOutputIdx <- 0 to numOutputs - 1){
      for(i <- 0 to vectorSize - 1){
        if(hidden_net(i) < 0.000000000001){
          hidden_net(i) = 0.0000000000001
        }
        layer0(i,inIdx) += -learningRate * errgrad1(i,netOutputIdx) * hidden_net(i) * (1 - hidden_net(i))
      }
    }
    
    (layer0,layer1,error)
  }
  
  def train_sg_test(): Unit = {
    var vectorSize = 3
    var layer0 = DenseMatrix.zeros(3, 4)
    var counter = 0.0
    for (i <- 0 to 2) {
      for (j <- 0 to 3) {
        counter += 0.1
        layer0(i, j) = counter
      }
    }
    println(layer0)

    var layer1 = DenseMatrix.zeros(4, 3)
    counter = 0.0
    for (i <- 0 to 3) {
      for (j <- 0 to 2) {
        counter += 0.1
        layer1(i, j) = counter
      }
    }
    println(layer1)
    
    var inIdx = 2

    // input -> hidden
    var hidden_net = DenseVector(Array.fill[Double](vectorSize)(0.0))
    for (i <- 0 to vectorSize - 1) {
      hidden_net(i) = layer0(i, inIdx)
    }

    println("hidden_net:" + hidden_net)

    var hidden_act = DenseVector(Array.fill[Double](vectorSize)(0.0))
    for (i <- 0 to vectorSize - 1) {
      hidden_act(i) = activation_function(hidden_net(i))
    }
    println("hidden_act:" + hidden_act)

    // hidden -> output
    var outIdx = 1
    var target = 1
    var learningRate = 0.1

    var output_net = 0.0
    for (i <- 0 to vectorSize - 1) {
      output_net += layer1(outIdx, i) * hidden_act(i)
    }
    var output_act = activation_function(output_net)
    println("output_net=" + output_net)
    println("output_act=" + output_act)

    println("error=" + (target - output_act) + " gt:" + target + " is:" + output_act)

    var output_error_gradient = (output_act - target) * (1 - output_act) * output_act
    println("output_error_gradient " + output_error_gradient)

    // backpropagation

    // layer1 update
    var errgrad1 = DenseVector(Array.fill[Double](vectorSize)(0.0))
    for (i <- 0 to vectorSize - 1) {
      layer1(outIdx, i) -= learningRate * output_error_gradient * hidden_act(i)
      errgrad1(i) += output_error_gradient * layer1(outIdx, i)
    }

    // layer0 update
    for (i <- 0 to vectorSize - 1) {
      layer0(i, inIdx) += learningRate * errgrad1(i) * hidden_net(i) * (1 - hidden_net(i))
    }

    println("new layer0:" + layer0)
    println("new layer1:" + layer1)



    println("==========================================================")
    hidden_net = DenseVector(Array.fill[Double](vectorSize)(0.0))
    for (i <- 0 to vectorSize - 1) {
      hidden_net(i) = layer0(i, inIdx)
    }
    hidden_act = DenseVector(Array.fill[Double](vectorSize)(0.0))
    for (i <- 0 to vectorSize - 1) {
      hidden_act(i) = activation_function(hidden_net(i))
    }
    output_net = 0.0
    for (i <- 0 to vectorSize - 1) {
      output_net += layer1(outIdx, i) * hidden_act(i)
    }
    output_act = activation_function(output_net)
    println("error=" + (target - output_act) + " gt:" + target + " is:" + output_act)

  }

  /*
  def trainNetworkParallel(vectorSize: Int,learningRate: Double, windowSize: Int, numIterations: Int, layer0 : DenseMatrix, layer1: DenseMatrix,sentenceInNumbers:DataSet[Array[Int]], vocabDS : DataSet[VocabWord]): (DenseMatrix,DenseMatrix) ={
    val expTable = createExpTable()
    println("collecting and training Network")
    val vocab : Array[VocabWord] = vocabDS.collect().toArray[VocabWord]
    println("collected!")

    var layer0New = layer0.copy
    var layer1New = layer1.copy
    var sentences = sentenceInNumbers.collect()


    var avrg_error = 0.0
    var error: Double = 0.0
    var maxit = 10000
    for(k <- 0 to maxit){
      var now : java.util.Date = new java.util.Date();
      println("k=" + k)

      var average_abs_error : Double = 0
      var its = 0

      val random = new XORShiftRandom(seed ^ /*((idx + 1) << 16) ^*/ ((-k - 1) << 8))

      val alpha = 0.01

      var t0 = System.nanoTime()

      for(sentence_counter <- 0  to sentences.length - 1){
        
        var timediff = System.nanoTime() - t0

        println("sentence_coutner:" + sentence_counter + " of " + (sentences.length -1) + ":" + timediff/1000/1000/1000.0 + "s")

        t0 = System.nanoTime()

        var sentence = sentences(sentence_counter)

        var sentence_position = 0
        // discard sentences with length 0, subsampling
        // TODO: remove and make distributed
        sentenceInNumbers.getExecutionEnvironment.getConfig.disableSysoutLogging()
        //var it = sentenceInNumbers.collect.iterator
        //var sentence : Array[Int] = it.next()



        //println("training for sentence:")
        //for(i <- 0 to sentence.length - 1){
        //  print(" " + vocab(sentence(i)).word)
        //}
        //println(" ")
        // iterate through all input words
        var last_it = false
        if(k == maxit){ last_it = true}
        if(last_it){
          for(ll <- 0 to sentence.length - 1){
            print(vocab(sentence(ll)).word + " ")
          }
          println(" ")
        }

        for (pos <- 0 to sentence.length - 1){
          // chose at random, words closer to the original word are more important
          var currentWindowSize = random.nextInt(windowSize - 1) + 1
          //var currentWindowSize = 3

          // go along
          for(outpos <- (- currentWindowSize + pos) to (pos + currentWindowSize)){
            if(outpos >= 0 && outpos != pos && outpos <= sentence.length - 1){
              //if(outpos == pos)  {
              //println("inpos:" + pos + ", outpos=" + outpos)
              val outIdx: Int = sentence(outpos)
              val inIdx: Int = sentence(pos)



              //println("training:" + inIdx + " to " + outIdx + " words:" + vocab(inIdx).word + " | " + vocab(outIdx).word)
              //if (inIdx >= 10000 && outIdx >= 100000) {
              if(last_it){
                println("training:" + vocab(inIdx).word + "  and   " + vocab(outIdx).word)
              }
              val res = train_sg_test_iterative(vocab,layer0New,layer1New,inIdx, outIdx,last_it)
              //var res = train_sg_iterative_matrix(expTable,vocab,layer0New,layer1New,inIdx, outIdx,last_it)
              layer0New = res._1
              layer1New = res._2
              error += res._3
              avrg_error += res._3
            }
          }
        }
      }
      if(k % 1000 == 0){
        println(k + ";"+ avrg_error / 1000.0 )//+ ":" + error)
        //println("layer0:" + layer0New)
        //println("layer1:" + layer1New)
        avrg_error = 0.0
        error = 0.0
      }
    }
    println("layer0:\n" + layer0New)
    println("layer1:\n" + layer1New)
    (layer0New,layer1New)
  }
  
  
  
  */

  def train_sentence(vocab : Array[VocabWord],layer0 : DenseMatrix,layer1 : DenseMatrix,sentence : Array[Int]): (DenseMatrix,DenseMatrix) ={
    var layer0New = layer0.copy
    var layer1New = layer1.copy
      for (pos <- 0 to sentence.length - 1) {
        // chose at random, words closer to the original word are more important
        var currentWindowSize = 4

        // go along
        for (outpos <- (-currentWindowSize + pos) to (pos + currentWindowSize)) {
          if (outpos >= 0 && outpos != pos && outpos <= sentence.length - 1) {

            val outIdx: Int = sentence(outpos)
            val inIdx: Int = sentence(pos)

            val res = train_sg_test_iterative(vocab, layer0New, layer1New, inIdx, outIdx, false)
            //var res = train_sg_iterative_matrix(expTable,vocab,layer0New,layer1New,inIdx, outIdx,last_it)
            layer0New = res._1
            layer1New = res._2
            //error += res._3
            //avrg_error += res._3
          }
        }
      }
    (layer0New,layer1New)
  }
  
  def trainNetwork_distributed(vectorSize: Int, learningRate: Double, windowSize: Int, numIterations: Int, layer0: DenseMatrix, layer1: DenseMatrix, sentenceInNumbers: DataSet[Array[Int]], vocabDS: DataSet[VocabWord]): (DenseMatrix, DenseMatrix) = {
    
    // additional parameter batchsize = 
    var batchsize = 100

    var sentencecount : Long = sentenceInNumbers.count

    // number of keys = sentencecounts / batchsize
    var num_keys : Long = sentencecount / batchsize

    println("num_keys:" + num_keys)


    var max = num_keys

    // (w1,w2)
    var weights : DataSet[(DenseMatrix,DenseMatrix)] = sentenceInNumbers.getExecutionEnvironment.fromElements((layer0,layer1))//env.fromElements((1,2))
    // touple (key,sentence)
    var sentences_withkeys :GroupedDataSet[(Int,Array[Int])] = sentenceInNumbers.map(new keyGeneratorFunction(max.toInt)).name("mapSentences").groupBy(0)


    var maxIterations : Int = 20
    //var iterativeOperator = weights.iterate(maxIterations)
    val finalWeights: DataSet[(DenseMatrix, DenseMatrix)] = weights.iterate(maxIterations)
    {
      previousWeights : DataSet[(DenseMatrix,DenseMatrix)] => {
        val nextWeights  : DataSet[(DenseMatrix,DenseMatrix)] = sentences_withkeys.reduceGroup {
          // comput updates of weight matrices per "class" / training data partition
          new RichGroupReduceFunction[(Int, Array[Int]), (DenseMatrix, DenseMatrix)] {
            override def reduce(values: Iterable[(Int, Array[Int])], out: Collector[(DenseMatrix, DenseMatrix)]): Unit = {
              var it = values.iterator()
              var iterativeWeights: (DenseMatrix, DenseMatrix) = getIterationRuntimeContext.getBroadcastVariable[(DenseMatrix, DenseMatrix)]("iterativeWeights").get(0)
              var vocab : DataSet[VocabWord] = getIterationRuntimeContext.getBroadcastVariable("vocab")
              // layer 0 and layer 1
              var layer0 = iterativeWeights._1
              var layer1 = iterativeWeights._2

              while (it.hasNext) {
                var sentence : Array[Int] = it.next()._2
                train_sentence(vocab ,layer0 : DenseMatrix,layer1 : DenseMatrix,sentence)
              }
              out.collect((iterativeWeights._1, iterativeWeights._2))
            }
          }
        }.name("reduceGroup->sentences_withKeys").withBroadcastSet(previousWeights, "iterativeWeights").withBroadcastSet(vocabDS,"vocab")
          .reduce(new ReduceFunction[(DenseMatrix, DenseMatrix)] {
          override def reduce(value1: (DenseMatrix, DenseMatrix), value2: (DenseMatrix, DenseMatrix)): (DenseMatrix, DenseMatrix) = {
            (value1._1,value2._2)
          }
        }).name("holger")
        nextWeights
      }
    }
    finalWeights.print()
    //println(finalWeights.getExecutionEnvironment.getExecutionPlan())



    finalWeights.first(1).collect()(0)

    /*
    val expTable = createExpTable()
    println("collecting and training Network")
    //val vocab: Array[VocabWord] = vocabDS.collect().toArray[VocabWord]
    println("collected!")

    var layer0New = sentenceInNumbers.getExecutionEnvironment.fromElements(layer0.copy)
    var layer1New = sentenceInNumbers.getExecutionEnvironment.fromElements(layer1.copy)


    var avrg_error = 0.0
    var error: Double = 0.0
    var maxit = 10000

    // TODO: change to iterate
    //sentenceInNumbers.iterate(10)
    
    var sentencesInNumbersIterative = sentenceInNumbers.iterate(maxit)
    { 
      iterationInput =>
          val result = sentenceInNumbers. map{ x => x}
        result
    }
    
    
    for (k <- 0 to maxit) {
      
      println("iteration:" + k + " of " + maxit)
      
      var partialResult: Seq[(DenseMatrix, DenseMatrix, Int)] = sentenceInNumbers.map(new RichMapFunction[Array[Int], (DenseMatrix, DenseMatrix, Int)] {
        def train_sg_test_iterative(vocab: util.ArrayList[VocabWord], layer0: DenseMatrix, layer1: DenseMatrix, inIdx: Int, outIdx: Int): (DenseMatrix, DenseMatrix, Double) = {
          var error = 0.0
          var learningRate = 0.1
          var vectorSize = layer0.numRows
          
          // input -> hidden
          var hidden_net = DenseVector(Array.fill[Double](vectorSize)(0.0))
          for (i <- 0 to vectorSize - 1) {
            hidden_net(i) = layer0(i, inIdx)
          }
          var hidden_act = DenseVector(Array.fill[Double](vectorSize)(0.0))
          for (i <- 0 to vectorSize - 1) {
            hidden_act(i) = hidden_net(i) //activation_function(hidden_net(i))
          }
          // hidden -> output
          var numOutputs = vocab.get(outIdx).codeLen
          var errgrad1 = DenseMatrix.zeros(vectorSize, numOutputs)
          for (netOutputIdx <- 0 to numOutputs - 1) {
            var netOutputNum = vocab.get(outIdx).point(netOutputIdx)
            var target = vocab.get(outIdx).code(netOutputIdx)
            var output_net = 0.0
            for (i <- 0 to vectorSize - 1) {
              output_net += layer1(netOutputNum, i) * hidden_act(i)
            }
            var output_act = activation_function(output_net)
            error += math.abs(output_act - target)
            var output_error_gradient = (output_act - target) * (1 - output_act) * output_act
            // backpropagation
            // layer1 update
            for (i <- 0 to vectorSize - 1) {
              layer1(netOutputNum, i) -= learningRate * output_error_gradient * hidden_act(i)
              errgrad1(i, netOutputIdx) += output_error_gradient * layer1(netOutputNum, i)
            }
          }
          // layer0 update
          for (netOutputIdx <- 0 to numOutputs - 1) {
            for (i <- 0 to vectorSize - 1) {
              if (hidden_net(i) < 0.000000000001) {
                hidden_net(i) = 0.0000000000001
              }
              layer0(i, inIdx) += -learningRate * errgrad1(i, netOutputIdx) * hidden_net(i) * (1 - hidden_net(i))
            }
          }
          (layer0, layer1, error)
        }


        override def map(value: Array[Int]): (DenseMatrix, DenseMatrix, Int) = {
          var vocab  = getRuntimeContext().getBroadcastVariable("vocabDS").asInstanceOf[util.ArrayList[VocabWord]]
          
          //var vocabDs: DataSet[VocabWord] = //getRuntimeContext().getBroadcastVariable("vocabDS") //this.getRuntimeContext.getBroadcastVariable[Array[VocabWord]]("vocab")
          //var vocab = vocabDs.collect()
          var windowSize: Int = 5 //getRuntimeContext().getBroadcastVariable[Int]("windowSize").get(0).asInstanceOf[Int]
          var layer0New: DenseMatrix = getRuntimeContext().getBroadcastVariable[DenseMatrix]("layer0").get(0).asInstanceOf[DenseMatrix]
          var layer1New: DenseMatrix = getRuntimeContext().getBroadcastVariable[DenseMatrix]("layer1").get(0).asInstanceOf[DenseMatrix]
          // train function
          //print("mapping!!")
          val random = new XORShiftRandom(seed ^ /*((idx + 1) << 16) ^*/ ((-k - 1) << 8))
          var sentence = value
          var wordcount = 0
          // iterate over all words as skipgram
          for (pos <- 0 to sentence.length - 1) {
            // chose at random, words closer to the original word are more important
            var currentWindowSize = random.nextInt(windowSize - 1) + 1
            // go along
            for (outpos <- (-currentWindowSize + pos) to (pos + currentWindowSize)) {
              if (outpos >= 0 && outpos != pos && outpos <= sentence.length - 1) {
                // get new positions input word as index of vocab and output word as index of vocab
                val outIdx: Int = sentence(outpos)
                val inIdx: Int = sentence(pos)
                //println("outIdx:" + outIdx + " inIdx:" + inIdx)
                wordcount += 1
                val res = train_sg_test_iterative(vocab, layer0New, layer1New, inIdx, outIdx)
                layer0New = res._1
                layer1New = res._2
                // error = res._3
              }
            }
          }

          (layer0New, layer1New, wordcount)
        }
      }).withBroadcastSet(vocabDS, "vocabDS").withBroadcastSet(layer0New, "layer0").withBroadcastSet(layer1New, "layer1").collect()
    }
    
    // TODO: combine output

      println("layer0:\n" + layer0New)
      println("layer1:\n" + layer1New)
    var dm1a : Array[DenseMatrix] = layer0New.collect.toArray[DenseMatrix]
    var dm1 : DenseMatrix = dm1a(0)
    var dm2a : Array[DenseMatrix] = layer1New.collect.toArray[DenseMatrix]
    var dm2 : DenseMatrix = dm2a(0)
    (dm1,dm2)
    
  */
  }
  
  

    /**
     * trains network with matrices
     */
    /*
    def trainNetwork2(vectorSize: Int, learningRate: Double, windowSize: Int, numIterations: Int, layer0: DenseMatrix, layer1: DenseMatrix, sentenceInNumbers: DataSet[Array[Int]], vocabDS: DataSet[VocabWord]): (DenseMatrix, DenseMatrix) = {
      val expTable = createExpTable()
      println("collecting and training Network")
      val vocab: Array[VocabWord] = vocabDS.collect().toArray[VocabWord]
      println("collected!")

      var layer0New = layer0.copy
      var layer1New = layer1.copy
      var sentences = sentenceInNumbers.collect()


      var avrg_error = 0.0
      var error: Double = 0.0
      var maxit = 10000
      for (k <- 0 to maxit) {
        var now: java.util.Date = new java.util.Date();
        println("k=" + k)

        var average_abs_error: Double = 0
        var its = 0

        val random = new XORShiftRandom(seed ^ /*((idx + 1) << 16) ^*/ ((-k - 1) << 8))

        val alpha = 0.01

        var t0 = System.nanoTime()

        for (sentence_counter <- 0 to sentences.length - 1) {

          var timediff = System.nanoTime() - t0

          println("sentence_coutner:" + sentence_counter + " of " + (sentences.length - 1) + ":" + timediff / 1000 / 1000 / 1000.0 + "s")

          t0 = System.nanoTime()

          var sentence = sentences(sentence_counter)

          var sentence_position = 0
          // discard sentences with length 0, subsampling
          // TODO: remove and make distributed
          sentenceInNumbers.getExecutionEnvironment.getConfig.disableSysoutLogging()
          //var it = sentenceInNumbers.collect.iterator
          //var sentence : Array[Int] = it.next()


          //println("training for sentence:")
          //for(i <- 0 to sentence.length - 1){
          //  print(" " + vocab(sentence(i)).word)
          //}
          //println(" ")
          // iterate through all input words
          var last_it = false
          if (k == maxit) {
            last_it = true
          }
          if (last_it) {
            for (ll <- 0 to sentence.length - 1) {
              print(vocab(sentence(ll)).word + " ")
            }
            println(" ")
          }

          for (pos <- 0 to sentence.length - 1) {
            // chose at random, words closer to the original word are more important
            var currentWindowSize = random.nextInt(windowSize - 1) + 1
            //var currentWindowSize = 3

            // go along
            for (outpos <- (-currentWindowSize + pos) to (pos + currentWindowSize)) {
              if (outpos >= 0 && outpos != pos && outpos <= sentence.length - 1) {
                //if(outpos == pos)  {
                //println("inpos:" + pos + ", outpos=" + outpos)
                val outIdx: Int = sentence(outpos)
                val inIdx: Int = sentence(pos)



                //println("training:" + inIdx + " to " + outIdx + " words:" + vocab(inIdx).word + " | " + vocab(outIdx).word)
                //if (inIdx >= 10000 && outIdx >= 100000) {
                if (last_it) {
                  println("training:" + vocab(inIdx).word + "  and   " + vocab(outIdx).word)
                }
                val res = train_sg_test_iterative(vocab, layer0New, layer1New, inIdx, outIdx, last_it)
                //var res = train_sg_iterative_matrix(expTable,vocab,layer0New,layer1New,inIdx, outIdx,last_it)
                layer0New = res._1
                layer1New = res._2
                error += res._3
                avrg_error += res._3
              }
            }
          }
        }
        if (k % 1000 == 0) {
          println(k + ";" + avrg_error / 1000.0) //+ ":" + error)
          //println("layer0:" + layer0New)
          //println("layer1:" + layer1New)
          avrg_error = 0.0
          error = 0.0
        }
      }
      println("layer0:\n" + layer0New)
      println("layer1:\n" + layer1New)
      (layer0New, layer1New)
    }
    */
    /**
     * trains network 
     */
    /*
    def trainNetwork(resultingParameters: ParameterMap, syn0Global: Array[Float], syn1Global: Array[Float], sentencesInNumbers: DataSet[Array[Int]], vocabDS: DataSet[VocabWord]): Unit = {

      //TODO: keep as DataSet
      println("collecting and training Network")
      val vocab: Array[VocabWord] = vocabDS.collect().toArray[VocabWord]
      println("collected!")
      var lr = resultingParameters.get[Double](LearningRate)
      var learningRate: Double = 0
      lr match {
        case Some(lR) => learningRate = lR
        case None => throw new Exception("Could not retrieve learning Rate, none specified?")
      }

      var ws = resultingParameters.get[Int](WindowSize)
      var windowSize: Int = 0
      ws match {
        case Some(wS) => windowSize = wS
        case None => throw new Exception("Could not retrieve window Size,none specified?")
      }

      var numI = resultingParameters.get[Int](NumIterations)
      var numIterations = 0
      numI match {
        case Some(ni) => numIterations = ni
        case None => throw new Exception("Could not retrieve number of Iterations, none specified?")
      }
      numIterations = 100000


      var vSize = resultingParameters.get[Int](VectorSize)
      var vectorSize = 0
      vSize match {
        case Some(vS) => vectorSize = vS
        case None => throw new Exception("Could not retrieve vector size of hidden layer, none specified?")
      }
      var word_count: Long = 0
      var last_word_count: Long = 0
      var alpha: Double = 0
      // global var?
      var word_count_actual: Long = 0;
      var iter = 5


      sentencesInNumbers.getExecutionEnvironment.getConfig.disableSysoutLogging()

      val expTable = createExpTable()
      // training Iterations
      // 1. we select one word as given by skipgram and counter origin as input
      // 2. we select one (random) word as given by skipgram window and position in sentence as training Target.
      // 3. -> compute gradient
      // 4. do backpropagation (for one output node only?)
      // 5. repeat 1 (do it for all sentences)
      for (k <- 1 to numIterations) {
        var average_abs_error: Double = 0
        var its = 0

        println(" " + k + " of " + numIterations + " is " + (k / numIterations * 100.0) + " % ")
        val random = new XORShiftRandom(seed ^ /*((idx + 1) << 16) ^*/ ((-k - 1) << 8))
        // set learning rate TODO: check if word count last etc is really necessairy
        alpha = 1
        if (word_count - last_word_count > 10000) {
          word_count_actual += word_count - last_word_count
          println("word_count_actual += word_count - last_word_count" + word_count_actual)
          last_word_count = word_count;


          //learning Rate with discount
          alpha = learningRate.toDouble * (1 - word_count_actual.toDouble / (iter * vocabSize + 1).toDouble)
          // dont let it get too small
          if (alpha < learningRate * 0.0001) {
            alpha = learningRate.toDouble * 0.0001
          }
        }
        println("alpha:" + alpha)
        //alpha = 0.1 //learningRate
        var sentence_position = 0
        // discard sentences with length 0, subsampling
        // TODO: remove and make distributed
        var it = sentencesInNumbers.collect.iterator
        var sentence: Array[Int] = it.next()

        var pos = 0
        //for (i <- 0 to sentence.length - 1) {println(sentence(i))}
        // go through sentence word by word
        while (pos < sentence.length) {
          val word = sentence(pos)

          // incorporate Skipgram (bi-gram with skipped words inbetween)
          var skipGramRandomWindowSize = random.nextInt(windowSize)

          var currentOutputWordWindowOffsetIdx = skipGramRandomWindowSize

          while (currentOutputWordWindowOffsetIdx < windowSize * 2 + 1 - skipGramRandomWindowSize) {
            // check if input and output word are different ( we only want to estimate context here)
            if (currentOutputWordWindowOffsetIdx != windowSize) {
              val currentOutputWordIdx = pos - windowSize + currentOutputWordWindowOffsetIdx

              if (currentOutputWordIdx >= 0 && currentOutputWordIdx < sentence.length) {
                val lastWord = sentence(currentOutputWordIdx)
                //println("lastWord= " + lastWord)
                // input transformation matrix index
                val l1 = lastWord * vectorSize

                // error gradient?
                val neu1e = new Array[Float](vectorSize)
                //Hierarchical Softmax?

                var numTreeDecisions = 0
                while (numTreeDecisions < vocab(word).codeLen) {
                  val inner = vocab(word).point(numTreeDecisions)

                  // vector offset output transformation matrix
                  val l2: Int = inner * vectorSize

                  var f: Double = 0
                  //propagate hidden -> output
                  for (c <- 0 to vectorSize) {
                    //println("wordcount=" + vocabSize)
                    f += syn0Global(c + l1) * syn1Global(c + l2)
                    //println("c="+c+" syn0Global("+c+" + "+l1+")="+syn0Global(c+l1)+" * syn1Global("+c+" + " + l2+")=" + syn1Global(c+ l2) + "=" + syn0Global(c + l1) * syn1Global(c + l2))
                  }
                  //println("output=" + f)

                  // check activation
                  if (f > -MAX_EXP && f < MAX_EXP) {
                    val ind = ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
                    f = expTable(ind)
                    //println("i=" + ind + " f= " + f)

                    // gradient
                    var g = ((1 - vocab(word).code(numTreeDecisions) - f)).toFloat
                    average_abs_error += math.abs(g)
                    g = (g * alpha).toFloat
                    its += 1
                    //println("alpha = " + alpha)
                    //println("g=" + g)// + " vocab(" + word + ").code(" + numTreeDecisions + ") = " + vocab(word).code(numTreeDecisions)) 
                    //println("g = " + g)
                    // Propagate errors output -> hidden
                    //print(vectorSize + " = " + neu1e.length)
                    for (i <- 0 to vectorSize - 1) neu1e(i) += g * syn1Global(i + l2);
                    // Learn weights hidden -> output
                    for (i <- 0 to vectorSize - 1) syn1Global(i + l2) += g * syn0Global(i + l1);


                  }
                  // Learn weights input -> hidden

                  // check output:
                  numTreeDecisions += 1
                }
                for (i <- 0 to vectorSize - 1) syn0Global(i + l1) += neu1e(i);
              }

            }
            currentOutputWordWindowOffsetIdx += 1
          }
          pos += 1
        }


        println("sum error:" + average_abs_error) //its.toDouble)
      }
    }*/
    /**
     * Main training function, receives DataSet[String] (of words(!), change this?)
     * @tparam T
     * @return
     */
    implicit def fitWord2vec[T <: Vector] = new FitOperation[Word2vec, String] {
      override def fit(instance: Word2vec, fitParameters: ParameterMap, input: DataSet[String])
      : Unit = {


        val resultingParameters = instance.parameters ++ fitParameters
        val minCount = resultingParameters(MinCount)


        var lr = resultingParameters.get[Double](LearningRate)
        var learningRate: Double = 0
        lr match {
          case Some(lR) => learningRate = lR
          case None => throw new Exception("Could not retrieve learning Rate, none specified?")
        }

        var ws = resultingParameters.get[Int](WindowSize)
        var windowSize: Int = 0
        ws match {
          case Some(wS) => windowSize = wS
          case None => throw new Exception("Could not retrieve window Size,none specified?")
        }

        var numI = resultingParameters.get[Int](NumIterations)
        var numIterations = 0
        numI match {
          case Some(ni) => numIterations = ni
          case None => throw new Exception("Could not retrieve number of Iterations, none specified?")
        }
        numIterations = 100000


        var vSize = resultingParameters.get[Int](VectorSize)
        var vectorSize = 0
        vSize match {
          case Some(vS) => vectorSize = vS
          case None => throw new Exception("Could not retrieve vector size of hidden layer, none specified?")
        }


        input.getExecutionEnvironment.getConfig.disableSysoutLogging()
        // Get different words and sort them for their frequency
        println("initializing vocab Hash")
        var vocab_hash = learnVocab(input, minCount)
        //huffman tree
        println("initializing vocab dataset")
        var vocabDS: DataSet[VocabWord] = vocab_hash._1
        var hash = vocab_hash._2
        var vocabIndices = vocab_hash._3
        println("creating binary tree")
        vocabDS = createBinaryTree(vocabDS)
        // convert words in sentences

        println("converting sentencewords to indices")
        var sentenceInNumber: DataSet[Array[Int]] = convertSentenceWordsToIndexes(input, hash) // this should be list of sentences (without period mark), with words separated by whitespace

        println("collecting sentences")
        var sn = sentenceInNumber.collect()




        //init net?
        println("initializing neural network")
        var (layer0, layer1) = initNetwork(resultingParameters)
        /*
        println(layer0)
        println(layer1)

        vocabDS.getExecutionEnvironment.getConfig.disableSysoutLogging()
        println(vocabDS.count)


        def minMax(b: Array[Array[Int]]) : (Int, Int) = {
          var min_glob = 10000
          var max_glob = -10000
          for(a <- b) {
            if(a.size != 0){
            var (min_loc,max_loc) = a.foldLeft((a(0), a(0))) { case ((min, max), e) => (math.min(min, e), math.max(max, e)) }
            if(min_glob > min_loc) min_glob = min_loc
            if(max_glob < max_loc) max_glob = max_loc
            }
          }
          (min_glob,max_glob)
        }
        var (min,max) = minMax(sn.toArray[Array[Int]])
        println("min:" + min)
        println("max:" + max)
        */
        //train_sg_test()
        println("training neural network")
        var res  = trainNetwork_distributed(vectorSize,learningRate, windowSize, numIterations, layer0, layer1,sentenceInNumber, vocabDS)
        
        
        
        //skipgram ?

        // train net
      }
    }

    implicit def transformVectors[T <: Vector : BreezeVectorConverter : TypeInformation : ClassTag] = {
      new TransformOperation[Word2vec, T, T] {
        override def transform(
                                instance: Word2vec,
                                transformParameters: ParameterMap,
                                input: DataSet[T])
        : DataSet[T] = {
          return null
        }
      }
    }
  
}
