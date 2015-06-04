
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


import java.lang.Iterable
import java.util

import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.feature_extraction.Word2vec._
import org.apache.flink.ml.math.{BLAS, Vector, BreezeVectorConverter}
import org.apache.flink.ml.pipeline.{TransformOperation, FitOperation, Transformer}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuilder}
import scala.reflect.ClassTag





import java.nio.ByteBuffer
import java.util.{Random => JavaRandom}

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
                              var codeLen: Int
                              ){
  override def toString(): String ={
    var points = ""
    for (i <- 0 to point.length - 1){points += point(i) + ","}
    points += point(point.length - 1)

    var codes = ""
    for (i <- 0 to code.length - 1){codes += code(i) + ","}
    codes += code(code.length - 1)

    var res = "VocabWord(" +
    " word:" + word +
    " cn:" + cn +
    " point:" + points +
    " code:" + codes +
    " codeLen:" + codeLen +")"
    
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
    override val defaultValue: Option[Int] = Some(100)
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
  
  private val MAX_CODE_LENGTH : Int = 40
  
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
  def learnVocab(words : DataSet[String], minCount: Int): (DataSet[VocabWord],DataSet[mutable.HashMap[String, Int]]) ={
    
    val vocab : DataSet[VocabWord] = words.flatMap(_.split(" "))
      .map{(_,1)}
      .groupBy(0).sum(1) //reduceByKey(_+_)
      .map(x => VocabWord(
      x._1,
      x._2,
      new Array[Int](MAX_CODE_LENGTH),
      new Array[Int](MAX_CODE_LENGTH),
      0))
      .filter(_.cn >= minCount)
      // avoid collect, could be a bottleneck since it runs on local vm and is defaulted to 512 mb.
      .setParallelism(1)
      .sortPartition("cn",Order.DESCENDING)
    
    val vocabIndices = vocab//.map{(_,1)}
      .reduceGroup {
            new GroupReduceFunction[VocabWord,(VocabWord,Int)]{
              override def reduce(values: Iterable[VocabWord], out: Collector[(VocabWord, Int)]): Unit = {
                val it = values.iterator()
                var counter = 0;
                
                while(it.hasNext ) {
                  val current = it.next()
                  out.collect((current, counter))
                  counter += 1
                }
              }
            } 
    }.setParallelism(1)
    
    
    val vocabHash2 = vocabIndices.reduceGroup{
      new GroupReduceFunction[(VocabWord,Int), mutable.HashMap[String, Int]]{
        override def reduce(values: Iterable[(VocabWord, Int)], out: Collector[mutable.HashMap[String, Int]]): Unit = {
          
          var outputHash : mutable.HashMap[String, Int] = mutable.HashMap.empty[String, Int]

          outputHash += "string" -> 2
          val it = values.iterator()
          var counter = 0;

          while(it.hasNext ) {
            var current:(VocabWord,Int) = it.next();
            
            outputHash +=  current._1.word -> current._2
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
    trainWordsCount = vocab.map(w1 => w1.cn).reduce((n1,n2) => n1+n2).collect()(0)

    (vocab,vocabHash2)
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

  def createBinaryTree(vocab :DataSet[VocabWord]): DataSet[VocabWord] = {
    
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

  
  def convertSentencesToHuffman(words : DataSet[String], hash : DataSet[mutable.HashMap[String, Int]]): DataSet[Array[Int]]  = {
  
    val sentencesStrings : DataSet[Array[String]] = words.flatMap{new FlatMapFunction[String,Array[String]] {
      override def flatMap(value: String, out: Collector[Array[String]]): Unit = {
        out.collect(value.split(" "))
      }
    }}
  
   val sentencesInts:DataSet[Array[Int]] = sentencesStrings.map{
     new RichMapFunction[Array[String],Array[Int]] {
      
       override def map(value: Array[String]): Array[Int] = {

        //val hash = getRuntimeContext.getBroadcastVariable("bcHash").get(0)
        val hashList : util.List[mutable.HashMap[String, Int]] = getRuntimeContext.getBroadcastVariable[mutable.HashMap[String,Int]]("bcHash")
        val hash : mutable.HashMap[String,Int] = hashList.get(0);
        
        var list = ListBuffer[Int]()

        for(word <- value){
          val wordInt = hash.get(word)
          wordInt match{
            case Some(w) => list.append(w)
            case None =>
          }
        }
        list.toArray
      }
     }
   }.withBroadcastSet(hash,"bcHash")
      
    sentencesInts
  }
  
  
  
  /**
   * initializes neural network to be trained to output word vectors
   */
  def initNetwork(resultingParameters : ParameterMap): (Array[Float],Array[Float]) ={
    //val initRandom = new XORShiftRandom(seed)
    
    val vectorSizeOpt = resultingParameters.get[Int](VectorSize)
    var vectorSize = 0
    
    vectorSizeOpt match{
      case Some(i) => vectorSize = i;
      case None => throw new RuntimeException("Vector size invalid!")
    }
    
    if (vocabSize * vectorSize * 8 >= Int.MaxValue){
      throw new RuntimeException("Too much information!!! " +
        "Please increase minCount or decrease vectorSize in Word2Vec +" +
        " to avoid an OOM. You are highly recommended to make your vocabSize*vectorSize," +
        "which is " + vocabSize + "*" + VectorSize + " for now, less than `Int.MaxValue/8`.")
    }

    val initRandom = new XORShiftRandom(seed)
    val syn0Global : Array[Float] =
      Array.fill[Float](vocabSize * vectorSize)((initRandom.nextFloat() - 0.5f) / vectorSize)
    val syn1Global : Array[Float]= new Array[Float](vocabSize * vectorSize)
    
    
    (syn0Global,syn1Global)
  }
  /**
   * trains network 
   */
  def trainNetwork(resultingParameters: ParameterMap, syn0Global:Array[Float],syn1Global:Array[Float], sentencesInNumbers: DataSet[Array[Int]], vocabDS : DataSet[VocabWord]): Unit ={
    
    //TODO: keep as DataSet
    println("collecting and training Network")
    val vocab : Array[VocabWord] = vocabDS.collect().toArray[VocabWord]
    println("collected!")
    var lr = resultingParameters.get[Double](LearningRate)
    var learningRate : Double = 0
    lr match{
      case Some(lR) => learningRate = lR
      case None => throw new Exception("Could not retrieve learning Rate, none specified?")
    }
    
    var ws = resultingParameters.get[Int](WindowSize)
    var windowSize : Int = 0
    ws match{
      case Some(wS) => windowSize = wS
      case None => throw new Exception("Could not retrieve window Size,none specified?")
    }
    
   var numI = resultingParameters.get[Int](NumIterations)
    var numIterations = 0
    numI match{
      case Some(ni) => numIterations = ni
      case None => throw new Exception("Could not retrieve number of Iterations, none specified?")
    }
    numIterations = 100000
    
    
    var vSize = resultingParameters.get[Int](VectorSize)
    var vectorSize = 0
    vSize match{
      case Some(vS) => vectorSize = vS
      case None => throw new Exception("Could not retrieve vector size of hidden layer, none specified?")
    }
    var word_count : Long = 0
    var last_word_count : Long = 0
    var alpha : Double = 0
    // global var?
    var word_count_actual : Long = 0;
    var iter = 5
    
    
    sentencesInNumbers.getExecutionEnvironment.getConfig.disableSysoutLogging()
    
    val expTable = createExpTable()
    // training Iterations
    // 1. we select one word as given by skipgram and counter origin as input
    // 2. we select one (random) word as given by skipgram window and position in sentence as training Target.
    // 3. -> compute gradient
    // 4. do backpropagation (for one output node only?)
    // 5. repeat 1 (do it for all sentences)
    for(k <- 1 to numIterations){
        var average_abs_error : Double = 0
        var its = 0
        
        println(" " + k + " of " + numIterations + " is " + (k/numIterations * 100.0) + " % ")
        val random = new XORShiftRandom(seed ^ /*((idx + 1) << 16) ^*/ ((-k - 1) << 8))
        // set learning rate TODO: check if word count last etc is really necessairy
      alpha = 1
        if (word_count - last_word_count > 10000){
          word_count_actual += word_count - last_word_count
          println("word_count_actual += word_count - last_word_count" + word_count_actual)
          last_word_count = word_count;
          
          
          //learning Rate with discount
          alpha = learningRate.toDouble * ( 1 - word_count_actual.toDouble/ (iter * vocabSize + 1).toDouble)
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
        var sentence : Array[Int] = it.next()
        
        var pos = 0
        //for (i <- 0 to sentence.length - 1) {println(sentence(i))}
        // go through sentence word by word
        while (pos < sentence.length){
          val word = sentence(pos)
        
          // incorporate Skipgram (bi-gram with skipped words inbetween)
          var skipGramRandomWindowSize = random.nextInt(windowSize)
          
          var currentOutputWordWindowOffsetIdx = skipGramRandomWindowSize
          
          while (currentOutputWordWindowOffsetIdx < windowSize * 2 + 1 - skipGramRandomWindowSize){
            // check if input and output word are different ( we only want to estimate context here)
            if (currentOutputWordWindowOffsetIdx != windowSize){
              val currentOutputWordIdx = pos - windowSize + currentOutputWordWindowOffsetIdx
              
              if(currentOutputWordIdx >= 0 && currentOutputWordIdx < sentence.length){
                  val lastWord = sentence(currentOutputWordIdx)
                  //println("lastWord= " + lastWord)
                  // input transformation matrix index
                  val l1 = lastWord * vectorSize
                  
                  // error gradient?
                  val neu1e = new Array[Float](vectorSize) 
                  //Hierarchical Softmax?
                  
                  var numTreeDecisions = 0
                  while( numTreeDecisions < vocab(word).codeLen){
                    val inner = vocab(word).point(numTreeDecisions)
                    
                    // vector offset output transformation matrix
                    val l2 : Int = inner * vectorSize
                    
                    var f : Double = 0 
                    //propagate hidden -> output
                    for(c <- 0 to vectorSize)
                    {
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
                      var g = ((1 - vocab(word).code(numTreeDecisions) - f) ).toFloat
                      average_abs_error += math.abs(g)
                      g =  (g * alpha).toFloat
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
            currentOutputWordWindowOffsetIdx +=1
          }
          pos += 1
        }
        
        
        println("sum error:" +  average_abs_error)//its.toDouble)
      }
  }
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
      
      // Get different words and sort them for their frequency
      var vocab_hash = learnVocab(input,minCount)
      //huffman tree
      var vocab : DataSet[VocabWord] = vocab_hash._1
      var hash = vocab_hash._2
      vocab = createBinaryTree(vocab)
      // convert words in sentences
    
      var sentencesInNumbers : DataSet[Array[Int]] = convertSentencesToHuffman(input,hash) // this should be list of sentences (without period mark), with words separated by whitespace
    
      //init net?
      var (syn0Global,syn1Global) = initNetwork(resultingParameters)
      
      trainNetwork(resultingParameters,syn0Global,syn1Global,sentencesInNumbers,vocab)
      // negative sampling -> use unigram
      
      //skipgram ?
      
      // train net
    }
  }
  
  implicit def transformVectors[T <: Vector: BreezeVectorConverter: TypeInformation: ClassTag] = {
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
