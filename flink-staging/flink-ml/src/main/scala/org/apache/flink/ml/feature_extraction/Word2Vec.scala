package org.apache.flink.ml.feature_extraction



import java.io.{ObjectInputStream, IOException}
import java.lang.Iterable
import java.text.SimpleDateFormat
import java.util
import java.util.{Random => JavaRandom}

import breeze.numerics.{abs, exp}
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.feature_extraction.Word2Vec._
import org.apache.flink.ml.math._
import org.apache.flink.ml.pipeline.{ FitOperation, Transformer}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.collection.mutable

import breeze._
import org.slf4j.LoggerFactory
import org.slf4j.Logger

import breeze.linalg.{Transpose, DenseMatrix => DenseMatrix}


import java.nio.ByteBuffer
import scala.util.Random
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

  // we need to just override next - this will be called by nextInt, nextFloat,
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
  var r : JavaRandom = new JavaRandom()//seed) //= 
  /*keyGeneratorFunction(max:Int){
    r = new scala.util.Random(seed)
    
  }//= new scala.util.Random(seed);
  */
  @throws(classOf[IOException])
  private def readObject(input:ObjectInputStream) : Unit = {
    input.defaultReadObject()
    r = new JavaRandom(seed)
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
                      ) {
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
 * For original C implementation, see https://code.google.com/p/Word2Vec/ 
 * For research papers, see 
 * Efficient Estimation of Word Representations in Vector Space
 * and 
 * Distributed Representations of Words and Phrases and their Compositionality.
 */
class Word2Vec
  extends Transformer[Word2Vec] {

  
  def setVectorSize(vectorSizeValue: Int): Word2Vec = {
    parameters.add(VectorSize, vectorSizeValue)
    this
  }

  def setLearningRate(learningRateValue: Float): Word2Vec = {
    parameters.add(LearningRate, learningRateValue)
    this
  }

  def setNumIterations(numIterationsValue: Int): Word2Vec = {
    parameters.add(NumIterations, numIterationsValue)
    this
  }

  def setMinCount(minCountValue: Int): Word2Vec = {
    parameters.add(MinCount, minCountValue)
    this
  }

  def setWindowSize(windowSizeValue: Int): Word2Vec = {
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

object Word2Vec {

  // ====================================== Parameters =============================================

  case object VectorSize extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(100)
  }

  case object LearningRate extends Parameter[Float] {
    override val defaultValue: Option[Float] = Some(0.1f)
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

  def apply(): Word2Vec = {
    new Word2Vec()
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
  def learnVocab(words: DataSet[String], minCount: Int): (DataSet[VocabWord],Seq[VocabWord]) = {//(DataSet[VocabWord], DataSet[mutable.HashMap[String, Int]], DataSet[(VocabWord, Int)]) = {

    var vocab: DataSet[VocabWord] = words//.flatMap(_.split(" "))//.filter(!_.isEmpty)) // also filters whitespace (they do not count as words)
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
      
      vocab = vocab.sortPartition("cn", Order.DESCENDING).setParallelism(1)
     // vocab = vocab.map(vw => VocabWord(vw.word,vw.cn,vw.point,vw.code,vw.codeLen,0))
    // number of distinct words in the corpus
    //vocabSize = vocab.count().toInt

    //println("total distinct words:" + vocabSize)
    
    var vocabCollected = vocab.collect()
    
    var it = vocabCollected.iterator
    var count = 0
    var totalWordCount = 0
    while(it.hasNext){//count <= vocabSize - 1){
      var el = it.next()
      //vocabHash += el.word -> count
      if(el.word == "thailand"){
        println("thailand is found at:" + count)
      }
      totalWordCount += el.cn
      count += 1
    }
    println("total words: " + totalWordCount)
    
    trainWordsCount = totalWordCount
    
    vocabSize = count
    require(vocabSize > 0, "The vocabulary size should be > 0. You may need to check " +
      "the setting of minCount, which could be large enough to remove all your words in sentences.")

    (vocab,vocabCollected)
  }


  /**
   * creates huffman tree from vocabulary
   * @param vocab
   * @return
   */
  def createBinaryTree(vocab: DataSet[VocabWord],vocabCollected : Seq[VocabWord]):DataSet[VocabWord] = {//vocab: DataSet[VocabWord]): DataSet[VocabWord] = {

    // mapping from word index to number of word-usage in corpus
    val count = new Array[Long](vocabSize * 2 + 1)

    val binary = new Array[Int](vocabSize * 2 + 1)
    val parentNode = new Array[Int](vocabSize * 2 + 1)
    val code = new Array[Int](MAX_CODE_LENGTH)
    val point = new Array[Int](MAX_CODE_LENGTH)
    var a = 0

    //val vocabCollected = vocab.collect() //TODO: remove?
    var vocabCollectedIt = vocabCollected.iterator
    while (a < vocabSize && vocabCollectedIt.hasNext) {
      val current = vocabCollectedIt.next()
      if(current.word == "thailand"){
        println("found thailand in tree at:" + a)
      }
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
      b = 0
      while (b < i) {
        current.code(i - b - 1) = code(b)
        current.point(i - b) = point(b) - vocabSize
        b += 1
      }
      a += 1
    }

    var vocabWordsDs = vocab.getExecutionEnvironment.fromCollection(vocabCollected)
    
    vocabWordsDs
  }

  /**
   * converts sentences of strings to sentences of indices (1-hot-encoded)
   */

  def mapSentences(input: DataSet[Iterable[String]]): DataSet[Array[Int]] = {

    // convert words inarray to 1-hot encoding
    val sentencesInts: DataSet[Array[Int]] = input.map {
      new RichMapFunction[Iterable[String], Array[Int]] {
        override def map(value: Iterable[String]): Array[Int] = {

          //val hash = getRuntimeContext.getBroadcastVariable("bcHash").get(0)
          val hashList: java.util.List[mutable.HashMap[String, Int]] = getRuntimeContext.getBroadcastVariable[mutable.HashMap[String, Int]]("bcHash")
          val hash: mutable.HashMap[String, Int] = hashList.get(0);

          var list = ListBuffer[Int]()

          var it = value.iterator()
          while(it.hasNext) {
            val wordInt = hash.get(it.next)
            wordInt match {
              case Some(w) => list.append(w)
              case None =>
            }
          }
          list.toArray
        }
      }
    }.withBroadcastSet(input.getExecutionEnvironment.fromElements(vocabHash), "bcHash")


    sentencesInts
  }

  def activation_function(in: Float): Float = {
    var out = 1.0f / (1.0f + exp(-in))
    out
  }

  def train_sg_test_iterative(vocab: java.util.ArrayList[VocabWord],layer0:breeze.linalg.DenseMatrix[Float],layer1:breeze.linalg.DenseMatrix[Float],inIdx:Int, outIdx:Int,last_it:Boolean):(breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float],Float) = {

    var error : Float = 0.0f
    var learningRate = 0.1f

    var vocabword = vocab.get(outIdx)
    var vectorSize = layer0.rows
    // feedforward
    
    // input -> hidden
    var l1 : breeze.linalg.DenseVector[Float] = layer0(::,inIdx) // hidden layer
    
    var neu1e : breeze.linalg.DenseVector[Float] = breeze.linalg.DenseVector.zeros[Float](vectorSize)
    
    for( pointsIdx <- 0 to vocabword.codeLen - 1){
      
      var outputNum = vocabword.point(pointsIdx)
      var target = vocabword.code(pointsIdx)
      
      
      var l_1 : Transpose[breeze.linalg.DenseVector[Float]] = layer1(outputNum,::)
      // hidden -> output 
      
      var in : Float =  l_1 * l1
      //var in : Float = rf
      var fa : Double = 1.0f /  (1.0f + breeze.numerics.exp(-in))
      
      error += breeze.numerics.abs(1.0f - fa - target).toFloat
      
      var g =  (1.0 - target - fa) * learningRate//(1.0 - target - fa) * learningRate
      
      neu1e := (layer1(outputNum,::) * g.toFloat).t :+ neu1e
      
      layer1(outputNum,::) := (g.toFloat * layer0(::,inIdx)).t :+ layer1(outputNum,::)
    }
    
    layer0(::,inIdx) := neu1e :+ layer0(::,inIdx)
    
    (layer0,layer1,error)
  }
  

  def train_sentence_non_optimized(vocab : java.util.ArrayList[VocabWord],layer0 : breeze.linalg.DenseMatrix[Float],layer1 : breeze.linalg.DenseMatrix[Float],sentence : Array[Int]): (breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float],Int,Double) ={
    var trainingCount = 0
    var total_error : Double = 0
    var layer0New = layer0
    var layer1New = layer1

    for (pos <- 0 to sentence.length - 1) {
      // chose at random, words closer to the original word are more important
      var currentWindowSize : Int = scala.util.Random.nextInt(5) + 1//5//scala.util.Random.nextInt(3) + 2
      // go along
      for (outpos <- (-currentWindowSize + pos) to (pos + currentWindowSize)) {
        if (outpos >= 0 && outpos != pos && outpos <= sentence.length - 1) {

          val outIdx: Int = sentence(outpos)
          val inIdx: Int = sentence(pos)

          val res = train_sg_test_iterative(vocab, layer0New, layer1New, inIdx, outIdx, false)

          layer0New = res._1
          layer1New = res._2
          total_error += res._3
          trainingCount += 1
        }
      }
    }

    var errr = total_error/trainingCount.toDouble
    if(errr.isNaN){
      errr = 0.0
      println("errorNaN! " + total_error + "/" + trainingCount.toDouble + " sentenceLength:" + sentence.length)
    }
    (layer0New,layer1New,trainingCount, errr)
  }
  
  def train_sentence(vocab : java.util.ArrayList[VocabWord],layer0 : breeze.linalg.DenseMatrix[Float],layer1 : breeze.linalg.DenseMatrix[Float],sentence : Array[Int]): (breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float],Int,Float) ={
    var trainingCount = 0
    var total_error : Float = 0
    var layer0New = layer0
    var layer1New = layer1
    
    for (pos <- 0 to sentence.length - 1) {
      // chose at random, words closer to the original word are more important
      var currentWindowSize : Int = scala.util.Random.nextInt(5) + 1//5//scala.util.Random.nextInt(3) + 2
      // go along
      for (outpos <- (-currentWindowSize + pos) to (pos + currentWindowSize)) {
        if (outpos >= 0 && outpos != pos && outpos <= sentence.length - 1) {

          val outIdx: Int = sentence(outpos)
          val inIdx: Int = sentence(pos)
          
          val res = train_sg_test_iterative(vocab, layer0New, layer1New, inIdx, outIdx, false)
          
          layer0New = res._1
          layer1New = res._2
          total_error += res._3
          trainingCount += 1
        }
      }
    }
    var errr : Float = total_error/trainingCount.toFloat
    (layer0New,layer1New,trainingCount, errr)
  }



  
 
  
  
  def trainNetwork_distributed_not_optimized(vectorSize: Int, learningRate: Double, windowSize: Int, numIterations: Int, layer0: breeze.linalg.DenseMatrix[Float], layer1: breeze.linalg.DenseMatrix[Float], sentenceInNumbers: DataSet[Array[Int]], vocabDS: DataSet[VocabWord]): (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float]) = {

    // additional parameter batchsize = 
    var batchsize = 1000

    var sentencecount : Long = sentenceInNumbers.count

    // number of keys = sentencecounts / batchsize
    var num_keys : Long = sentencecount / batchsize

    println("num_keys:" + num_keys)

    var max = num_keys

    // (w1,w2)
    var weights : DataSet[(breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float],Int)] = sentenceInNumbers.getExecutionEnvironment.fromElements((layer0,layer1,0))//env.fromElements((1,2))
    // touple (key,sentence)
    var sentences_withkeys :GroupedDataSet[(Int,Array[Int])] = sentenceInNumbers.map(new keyGeneratorFunction(max.toInt)).name("mapSentences").groupBy(0)


    var maxIterations : Int = 20
    //var iterativeOperator = weights.iterate(maxIterations)
    val finalWeights: DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)] = weights.iterate(maxIterations)
    {
      previousWeights : DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)] => {
        val nextWeights  : DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)] = sentences_withkeys.reduceGroup {
          // comput updates of weight matrices per "class" / training data partition
          new RichGroupReduceFunction[(Int, Array[Int]), (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)] {
            override def reduce(values: Iterable[(Int, Array[Int])], out: Collector[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)]): Unit = {
              var it = values.iterator()
              var iterativeWeights: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int) = getIterationRuntimeContext.getBroadcastVariable[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)]("iterativeWeights").get(0)
              var vocab : java.util.ArrayList[VocabWord] = getIterationRuntimeContext.getBroadcastVariable("vocab").asInstanceOf[java.util.ArrayList[VocabWord]]


              // layer 0 and layer 1
              var layer0 = iterativeWeights._1
              var layer1 = iterativeWeights._2

              var trainCounts = 0
              while (it.hasNext) {
                var sentence : Array[Int] = it.next()._2
                var res = train_sentence(vocab ,layer0 : breeze.linalg.DenseMatrix[Float],layer1 : breeze.linalg.DenseMatrix[Float],sentence)
                layer0 = res._1
                layer1 = res._2
                trainCounts += res._3
              }
              out.collect((iterativeWeights._1, iterativeWeights._2,trainCounts))//,trainCounts))
            }
          }
        }.name("reduceGroup->sentences_withKeys").withBroadcastSet(previousWeights, "iterativeWeights").withBroadcastSet(vocabDS,"vocab")
          .reduce(new ReduceFunction[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)] {
          override def reduce(value1: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int), value2: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)): (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int) = {


            var w1 = value1._3.toFloat
            var w2 = value2._3.toFloat
            if(w1 == 0){println("w1==" + w1)
              return( (value2._1,value2._2,w2.toInt) )
            }
            if(w2 == 0){println("w2==" + w2)
              return( (value1._1,value2._2,w1.toInt))
            }

            var total = w1 + w2

            var l1: breeze.linalg.DenseMatrix[Float] = value1._1 * w1
            l1 = value2._1 * w2
            var l2: breeze.linalg.DenseMatrix[Float] = value1._2 * w1
            l2 = value2._2 * w2
            l1 = l1 / total
            l2 = l2 / total


            (l1,l2,total.toInt)
          }
        }).name("holger")
        println("iteration done!")
        nextWeights
      }
    }
    //finalWeights.print()

    //output
    var res = finalWeights.first(1).collect()(0)
    (res._1,res._2)
  }
  
  
  def trainNetwork_distributed(vectorSize: Int, learningRate: Double, windowSize: Int, numIterations: Int, layer0: breeze.linalg.DenseMatrix[Float], layer1: breeze.linalg.DenseMatrix[Float], sentenceInNumbers: DataSet[Array[Int]], vocabDS: DataSet[VocabWord]): (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float]) = {
    
    println("training distributed")
    
    // additional parameter batchsize = 
    var batchsize = 1000

    var sentencecount : Long = sentenceInNumbers.count

    // number of keys = sentencecounts / batchsize
    var num_keys : Long = sentencecount / batchsize

    println("num_keys:" + num_keys)
    
    var max = num_keys

    // (w1,w2)
    var weights : DataSet[(breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float])] = sentenceInNumbers.getExecutionEnvironment.fromElements((layer0,layer1))//env.fromElements((1,2))
    // touple (key,sentence)
    var sentences_withkeys :GroupedDataSet[(Int,Array[Int])] = sentenceInNumbers.map(new keyGeneratorFunction(max.toInt)).name("mapSentences").groupBy(0)


    var maxIterations : Int = 1
    //var iterativeOperator = weights.iterate(maxIterations)
    val finalWeights: DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float])] = weights.iterate(maxIterations)
    {
      previousWeights : DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float])] => {
        val nextWeights  : DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float])] = sentences_withkeys.reduceGroup {
          // comput updates of weight matrices per "class" / training data partition
          new RichGroupReduceFunction[(Int, Array[Int]), (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)] {
            override def reduce(values: Iterable[(Int, Array[Int])], out: Collector[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)]): Unit = {
              var it = values.iterator()
              var iterativeWeights: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float]) = getIterationRuntimeContext.getBroadcastVariable[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float])]("iterativeWeights").get(0)
              var vocab : java.util.ArrayList[VocabWord] = getIterationRuntimeContext.getBroadcastVariable("vocab").asInstanceOf[java.util.ArrayList[VocabWord]]
              
              // layer 0 and layer 1
              var layer0 = iterativeWeights._1
              var layer1 = iterativeWeights._2

              var trainCounts = 0
              while (it.hasNext) {
                var sentence : Array[Int] = it.next()._2
                var res = train_sentence(vocab ,layer0 : breeze.linalg.DenseMatrix[Float],layer1 : breeze.linalg.DenseMatrix[Float],sentence)
                layer0 = res._1
                layer1 = res._2
                trainCounts += res._3
              }
              out.collect((iterativeWeights._1, iterativeWeights._2,trainCounts))//,trainCounts))
            }
          }
        }.name("reduceGroup->sentences_withKeys").withBroadcastSet(previousWeights, "iterativeWeights").withBroadcastSet(vocabDS,"vocab")
          .reduce(new ReduceFunction[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)] {
          override def reduce(value1: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int), value2: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)): (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int) = {
            
            var w1 = value1._3.toFloat
            var w2 = value2._3.toFloat
            var total = w1 + w2
            var l1: breeze.linalg.DenseMatrix[Float] = value1._1 * w1
            l1 = l1 :+ value2._1 * w2
            var l2: breeze.linalg.DenseMatrix[Float] = value1._2 * w1
            l2 = l2 :+ value2._2 * w2
            l1 = l1 / total
            l2 = l2 / total

            (l1,l2,total.toInt)
          }
        }).map(x => (x._1,x._2))
        println("iteration done!")
        nextWeights
      }
    }
    //finalWeights.print()

    //output
    var res = finalWeights.first(1).collect()(0)
    (res._1,res._2)
  }
  
  def train_sg_smart_aggregate(alpha : Float, vocab: java.util.ArrayList[VocabWord],layer0:breeze.linalg.DenseMatrix[Float],layer1:breeze.linalg.DenseMatrix[Float],inIdx:Int, outIdx:Int):(breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float],Float) = {

    var error : Float = 0.0f

    var vocabword = vocab.get(outIdx)
    var vectorSize = layer0.rows
    // feedforward

    // input -> hidden
    var l1 : breeze.linalg.DenseVector[Float] = layer0(::,inIdx) // hidden layer

    var neu1e : breeze.linalg.DenseVector[Float] = breeze.linalg.DenseVector.zeros[Float](vectorSize)

    for( pointsIdx <- 0 to vocabword.codeLen - 1){

      var outputNum = vocabword.point(pointsIdx)
      var target = vocabword.code(pointsIdx)
      
      var l_1 : Transpose[breeze.linalg.DenseVector[Float]] = layer1(outputNum,::)
      // hidden -> output 

      var in : Float =  l_1 * l1
      //var in : Float = rf
      var fa : Double = 1.0f /  (1.0f + breeze.numerics.exp(-in))

      error += breeze.numerics.abs(1.0f - fa - target).toFloat

      var g =  (1.0 - target - fa) * alpha//(1.0 - target - fa) * learningRate

      neu1e := (layer1(outputNum,::) * g.toFloat).t :+ neu1e

      layer1(outputNum,::) := (g.toFloat * layer0(::,inIdx)).t :+ layer1(outputNum,::)
    }

    layer0(::,inIdx) := neu1e :+ layer0(::,inIdx)

    (layer0,layer1,error)
  }
  
  
  def train_sentence_smart_aggregate(learningRate: Float, vocab : java.util.ArrayList[VocabWord],layer0 : breeze.linalg.DenseMatrix[Float],layer1 : breeze.linalg.DenseMatrix[Float],sentence : Array[Int]):  (breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float],Int,Float,mutable.MutableList[Int]) ={
    var trainingCount = 0
    var total_error : Float = 0
    var layer0New = layer0
    var layer1New = layer1
    var alpha = learningRate
    /*var alpha = learningRate * (sentence.length.toFloat / trainWordsCount ).toFloat
    if(alpha < 0.0001f){
      alpha = 0.0001f
    }*/
    
    var layerModifications = mutable.MutableList[Int]()
    for (pos <- 0 to sentence.length - 1) {
      // we train on every word that occurs
      layerModifications = (layerModifications :+ sentence(pos))
      var l1ModificationsList : mutable.MutableList[Int] = mutable.MutableList[Int]() ++ vocab.get(sentence(pos)).point

      for (i <- 0 to l1ModificationsList.length - 1) {
        l1ModificationsList(i) = l1ModificationsList(i) + vocabSize
      }
      //layer1mods = layer1mods ++ vocab.get(sentence(pos)).point.toList// extends this list with contents of the array//vocab(sentence(pos)).points
      // chose at random, words closer to the original word are more important
      var currentWindowSize : Int = scala.util.Random.nextInt(5) + 1//5//scala.util.Random.nextInt(3) + 2
      // go along
      for (outpos <- (-currentWindowSize + pos) to (pos + currentWindowSize)) {
        if (outpos >= 0 && outpos != pos && outpos <= sentence.length - 1) {

          val outIdx: Int = sentence(outpos)
          val inIdx: Int = sentence(pos)

          val res = train_sg_smart_aggregate(alpha, vocab, layer0New, layer1New, inIdx, outIdx)

          layer0New = res._1
          layer1New = res._2
          total_error += res._3
          trainingCount += 1
        }
      }
    }
    layerModifications = layerModifications.distinct
    
    var errr : Float = total_error/trainingCount.toFloat
    (layer0New,layer1New,trainingCount, errr,layerModifications)
  }

  def trainNetwork_distributed_smart_aggregate(vectorSize: Int, learningRate: Float, windowSize: Int, numIterations: Int, layer0: breeze.linalg.DenseMatrix[Float], layer1: breeze.linalg.DenseMatrix[Float], sentenceInNumbers: DataSet[Array[Int]], vocabDS: DataSet[VocabWord]): (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float]) = {

    println("training distributed with smart aggregate")

    // additional parameter batchsize = 
    var batchsize = 1000

    var sentencecount : Long = sentenceInNumbers.count

    // number of keys = sentencecounts / batchsize
    var num_keys : Long = sentencecount / batchsize

    println("num_keys:" + num_keys)

    var max = num_keys

    var learningRateDS : DataSet[Float] = sentenceInNumbers.getExecutionEnvironment.fromElements(learningRate)
    
    // (w1,w2)
    var weights : DataSet[(breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float])] = sentenceInNumbers.getExecutionEnvironment.fromElements((layer0,layer1))//env.fromElements((1,2))
    // touple (key,sentence)
    var sentences_withkeys :GroupedDataSet[(Int,Array[Int])] = sentenceInNumbers.map(new keyGeneratorFunction(max.toInt)).name("mapSentences").groupBy(0)

    var maxIterations : Int = 1
    //var iterativeOperator = weights.iterate(maxIterations)
    val finalWeights: DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float])] = weights.iterate(maxIterations)
    {
      previousWeights : DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float])] => {
        val updates = sentences_withkeys.reduceGroup {
          // comput updates of weight matrices per "class" / training data partition
          new RichGroupReduceFunction[(Int, Array[Int]), (Int,breeze.linalg.DenseVector[Float])] {
            override def reduce(values: Iterable[(Int, Array[Int])], out: Collector[(Int,breeze.linalg.DenseVector[Float])]): Unit = {//(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)]): Unit = {
              var it = values.iterator()
              var iterativeWeights: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float]) = getIterationRuntimeContext.getBroadcastVariable[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float])]("iterativeWeights").get(0)
              var vocab : java.util.ArrayList[VocabWord] = getIterationRuntimeContext.getBroadcastVariable("vocab").asInstanceOf[java.util.ArrayList[VocabWord]]

              var learningRateLocal : Float = getIterationRuntimeContext.getBroadcastVariable("learningRate").asInstanceOf[Float]
              // layer 0 and layer 1
              var layer0 = iterativeWeights._1
              var layer1 = iterativeWeights._2

              var trainCounts = 0
              
              //TODO: add weighting according to word number of occurences for each training?
              
              var layerModificationsList : mutable.MutableList[Int] = mutable.MutableList[Int]()
              while (it.hasNext) {
                var sentence : Array[Int] = it.next()._2
                var res = train_sentence_smart_aggregate(learningRateLocal, vocab ,layer0 : breeze.linalg.DenseMatrix[Float],layer1 : breeze.linalg.DenseMatrix[Float],sentence)
                layer0 = res._1
                layer1 = res._2
                trainCounts += res._3
                layerModificationsList = layerModificationsList ++ res._5
              }
              
              // collect changes only
              
              // only distinct changes:
              layerModificationsList = layerModificationsList.distinct
              
              // collect relevant changes from matrix
              for(i <- 0  to layerModificationsList.length - 1){
                var r = layerModificationsList(i)
                if(r > vocabSize - 1){
                  // layer1
                  out.collect((r,layer1(r - vocabSize,::).t))
                }  else {
                  out.collect((r,layer0(::,r)))
                }
              }
              //out.collect((iterativeWeights._1, iterativeWeights._2,trainCounts))//,trainCounts))
            }
          }
        }.name("reduceGroup->sentences_withKeys").withBroadcastSet(previousWeights, "iterativeWeights").withBroadcastSet(vocabDS,"vocab")
          .withBroadcastSet(learningRateDS,"learningRate")
          .groupBy(0).reduceGroup{new GroupReduceFunction[(Int,breeze.linalg.DenseVector[Float]),(Int,breeze.linalg.DenseVector[Float])]{
          override def reduce(values: Iterable[(Int, linalg.DenseVector[Float])], out: Collector[(Int, linalg.DenseVector[Float])]): Unit = {
            var it = values.iterator()
            var ind : Int = 0
            var res :breeze.linalg.DenseVector[Float] = null
            
            if(it.hasNext()) {
              
              var f = it.next()
              var ind = f._1
              var first = f._2
              var res: breeze.linalg.DenseVector[Float] = first

            }else{

              return

            }
            while(it.hasNext){
              var changeVector = it.next()._2
              res =( res + changeVector) / 2.0f
            }
            out.collect((ind,res))
          }
        }}.collect()//.map(x => (breeze.linalg.DenseMatrix.zeros[Float](3,3),breeze.linalg.DenseMatrix.zeros[Float](3,3)))

        //var nextWeights = updates
        // put together matrices for next iteration
        var it = updates.iterator
        while(it.hasNext){
          var el = it.next
          if(el._1 > vocabSize - 1){
            // layer 1
            layer1(el._1 - vocabSize,::) := el._2.t
          }else{
            layer0(::,el._1) := el._2
          }
        }
        
        
        var executionEnv = sentences_withkeys.first(1).getExecutionEnvironment
        val nextWeights  : DataSet[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float])] = executionEnv.fromElements[(breeze.linalg.DenseMatrix[Float],breeze.linalg.DenseMatrix[Float])]((layer0,layer1))
          
          
          /*.reduce(new ReduceFunction[(breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)] {
          override def reduce(value1: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int), value2: (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int)): (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float],Int) = {

            var w1 = value1._3.toFloat
            var w2 = value2._3.toFloat
            var total = w1 + w2
            var l1: breeze.linalg.DenseMatrix[Float] = value1._1 * w1
            l1 = l1 :+ value2._1 * w2
            var l2: breeze.linalg.DenseMatrix[Float] = value1._2 * w1
            l2 = l2 :+ value2._2 * w2
            l1 = l1 / total
            l2 = l2 / total

            (l1,l2,total.toInt)
          }
        }).map(x => (x._1,x._2))*/
        println("iteration done!")
        nextWeights
      }
    }
    //finalWeights.print()

    //output
    var res = finalWeights.first(1).collect()(0)
    (res._1,res._2)
  }
  def trainNetwork_iterative(vectorSize: Int, learningRate: Double, windowSize: Int, numIterations: Int, layer0: breeze.linalg.DenseMatrix[Float], layer1: breeze.linalg.DenseMatrix[Float], sentenceInNumbers: DataSet[Array[Int]], vocabDS: DataSet[VocabWord]): (breeze.linalg.DenseMatrix[Float], breeze.linalg.DenseMatrix[Float]) = {
    
    println("training iterative")
    
    // additional parameter batchsize = 
    var batchsize = 100

    var sentencecount : Long = sentenceInNumbers.count

    // number of keys = sentencecounts / batchsize
    var num_keys : Long = sentencecount / batchsize

    var maxIterations : Int = 5
    //var iterativeOperator = weights.iterate(maxIterations)
    var vocabSeq:Seq[VocabWord] =  vocabDS.collect()
    var vocab: java.util.ArrayList[VocabWord] = new java.util.ArrayList[VocabWord]()
    
    for(i <- 0 to vocabSeq.length - 1){
      vocab.add(vocabSeq(i))
    }
    
    var sentences_collected = sentenceInNumbers.collect()
    
    var layer0New = layer0.copy
    var layer1New = layer1.copy
    
    
    var total_error = 0.0
    for(i <- 0 to maxIterations - 1){
      val t0 = System.nanoTime()
      for(j <- 0  to sentences_collected.length - 1) {
        var sentence = sentences_collected(j)
        var res = train_sentence(vocab, layer0New.copy, layer1New.copy, sentence)
        layer0New = res._1
        layer1New = res._2
        total_error += res._4
      }
      val t1 = System.nanoTime()
      println("iteration:" + i + " error:" + total_error + " elapsed time:" + (t1 - t0) / 1000000000.0 + " ns")
      total_error = 0
    }
    if(layer0New == layer0){
      println("layer0 and trained layer0 are the same!!")
    }
    if(layer1New == layer1){
      println("layer1 and trained layer1 are the same!!")
    }
    
    (layer0New.copy,layer1New.copy)
  }
  /**
   * Main training function, receives DataSet[String] (of words(!), change this?)
   * @return
   */
  implicit def fitWord2Vec = new FitOperation[Word2Vec, Array[String]] {
    override def fit(instance: Word2Vec, fitParameters: ParameterMap, input: DataSet[Array[String]])
    : Unit = {
      
      
      // remove sentences that have length 1 (just words without context)
      println("before filtering:" + input.count())
      var input_filtered = input.filter(_.length > 1)
      println("filtered:" + input_filtered.count)
      var words = input_filtered.flatMap(new FlatMapFunction[Array[String],String] {
        override def flatMap(value: Array[String], out: Collector[String]): Unit = {
          var it = value.iterator
          while(it.hasNext){
            out.collect(it.next())
          }
        }
      })
      
      // load parameters
      val resultingParameters = instance.parameters ++ fitParameters
      val minCount = resultingParameters(MinCount)

      var lr = resultingParameters.get[Float](LearningRate)
      var learningRate: Float = 0
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
      
      var vSize = resultingParameters.get[Int](VectorSize)
      var vectorSize = 0
      vSize match {
        case Some(vS) => vectorSize = vS
        case None => throw new Exception("Could not retrieve vector size of hidden layer, none specified?")
      }


      input_filtered.getExecutionEnvironment.getConfig.disableSysoutLogging()
      // Get different words and sort them for their frequency
      println("initializing vocab")
      var res1 = learnVocab(words, minCount)
      var vocabDS = res1._1
      var vocabCollected1 = res1._2
      println("creating binary tree")
      vocabDS = createBinaryTree(vocabDS,vocabCollected1)
      
      //vocabDS.print
      var vocabCollected = vocabDS.collect()
      var it = vocabCollected.iterator
      var count = 0
      while(it.hasNext){
        var el = it.next()
        vocabHash += el.word -> count
        count += 1
      }
      
      var before_thailand = vocabHash.get("thailand")
      
      // map sentences to DataSet[Array[Int]]
      // convert words inarray to 1-hot encoding
      var sentencesInts: DataSet[Array[Int]] = input_filtered.map {
        new RichMapFunction[Array[String], Array[Int]] {
          override def map(value: Array[String]): Array[Int] = {

            //val hash = getRuntimeContext.getBroadcastVariable("bcHash").get(0)
            val hashList: java.util.List[mutable.HashMap[String, Int]] = getRuntimeContext.getBroadcastVariable[mutable.HashMap[String, Int]]("bcHash")
            val hash: mutable.HashMap[String, Int] = hashList.get(0);

            var list = ListBuffer[Int]()
            
            var it = value.iterator
            while(it.hasNext) {
              val wordInt = hash.get(it.next())
              
              wordInt match {
                case Some(w) => list.append(w)
                case None =>
              }
            }
            list.toArray
          }
        }
      }.withBroadcastSet(input_filtered.getExecutionEnvironment.fromElements(vocabHash), "bcHash")

      sentencesInts = sentencesInts.filter(_.length > 1)

      //Seq.fill(vectorSize,vocabSize)(Random.nextInt(1000).toFloat - 500.0f / 500.0f)// - breeze.linalg.DenseMatrix.rand[Float](vectorSize,vocabSize)
     
      var layer0 : breeze.linalg.DenseMatrix[Float] = breeze.linalg.DenseMatrix.tabulate[Float](vectorSize, vocabSize){case (i, j) =>( Random.nextInt(1000).toFloat - 500.0f) / 500.0f}//breeze.linalg.DenseMatrix.rand[Double](vectorSize,vocabSize).asInstanceOf[breeze.linalg.DenseMatrix[Float]]//,Seq.fill(vectorSize,vocabSize)(Random.nextInt(1000).toFloat - 500.0f / 500.0f).toArray[Float])// - breeze.linalg.DenseMatrix.rand[Float](vectorSize,vocabSize)
      var layer1 : breeze.linalg.DenseMatrix[Float] = breeze.linalg.DenseMatrix.tabulate[Float](vocabSize,vectorSize){case (i, j) => ( Random.nextInt(1000).toFloat - 500.0f) / 500.0f}//breeze.linalg.DenseMatrix.rand[Double](vectorSize,vocabSize).asInstanceOf[breeze.linalg.DenseMatrix[Float]]//,Seq.fill(vectorSize,vocabSize)(Random.nextInt(1000).toFloat - 500.0f / 500.0f).toArray[Float])// - breeze.linalg.DenseMatrix.rand[Float](vectorSize,vocabSize)
      //breeze.linalg.DenseMatrix.rand[Double](vocabSize,vectorSize).asInstanceOf[breeze.linalg.DenseMatrix[Float]]// - breeze.linalg.DenseMatrix.rand[Float](vocabSize,vectorSize)

      //var res = trainNetwork_distributed_not_optimized(vectorSize,learningRate,windowSize,1,layer0,layer1,sentencesInts,vocabDS )
      //var res = trainNetwork_distributed(vectorSize,learningRate,windowSize,1,layer0,layer1,sentencesInts,vocabDS )
      //var res = trainNetwork_iterative(vectorSize,learningRate,windowSize,1,layer0,layer1,sentencesInts,vocabDS )
      
      var res = trainNetwork_distributed_smart_aggregate(vectorSize,learningRate,windowSize,1,layer0,layer1,sentencesInts,vocabDS)
      
      
      layer0 = res._1.copy
      println("done training network")
      // construct word-> vec map
      word2VecMap = mutable.HashMap.empty[String, breeze.linalg.DenseMatrix[Float]]
      
      var vocab = vocabDS.collect().sortWith(_.ind < _.ind)
      
      var i = 0
      for (i <- 0 to vocabSize -1) {
        val word = vocab(i).word
        val vector = layer0(::,i)
        word2VecMap += word -> vector.toDenseMatrix
      }
      
      // compute word vector norms:
      
      //var normalizedLayer0 = breeze.linalg.normalize(layer0,breeze.linalg.Axis._0,2)
      println("layer0:\n" + layer0)
      normsLayer0 = breeze.linalg.norm(layer0,breeze.linalg.Axis._0,2).toDenseVector.mapValues(_.toFloat)
      wordVecs = layer0.copy
      vocabGlob = vocab
      
      println("looking for china")
      // get word for china
      var ch = word2VecMap.get("china")
      var china = breeze.linalg.DenseVector(0.0).asInstanceOf[breeze.linalg.DenseVector[Float]]
      ch match {
        case Some(ni) => china = ni.toDenseVector.asInstanceOf[breeze.linalg.DenseVector[Float]]
        case None => throw new Exception("Could not retrieve word vector for China")
      }
      //china  = breeze.linalg.normalize(china,2)//breeze.linalg.normalize(china,breeze.linalg.Axis._0,2)
      
      //println(normalizedLayer0.rows + ";" + normalizedLayer0.cols)
      var normChina = breeze.linalg.norm(china,2)
      var cosDists : breeze.linalg.DenseVector[Float] = breeze.linalg.DenseVector.zeros[Float](vocabSize)
      for(i <- 0 to vocabSize - 1){
        cosDists(i) =   wordVecs(::,i).t * china
      }

      var resList: ListBuffer[(String,Double,breeze.linalg.DenseVector[Float])] = new ListBuffer[(String,Double,breeze.linalg.DenseVector[Float])]()

      println("norms layer0 = \n" + normsLayer0)
      for(i <- 0 to vocabSize -1){
        resList += new Tuple3(vocab(i).word, cosDists(i) / (normsLayer0(i) * normChina)  ,layer0(::,i).toDenseVector )
      }
      resList = resList.sortBy(_._2).reverse
      for(i <- 0 to 5){//vocabSize - 1){
        println(resList(i))
      }
      findSimilar("china")
      findSimilar("child")
      findSimilar("computer")
      findSimilar("dog")
    }
    var word2VecMap = mutable.HashMap.empty[String, breeze.linalg.DenseMatrix[Float]]
    var wordVecs : breeze.linalg.DenseMatrix[Float] = null
    var normsLayer0 : breeze.linalg.DenseVector[Float] = null
    var vocabGlob : Seq[VocabWord] = null
    /**
     * computes cosine distance
     */
    def cosineDist(): Unit = {
      
    }
    
    /**
     * finds similar vectors (synonyms)
     */
    def findSimilar(inputWord : String): Unit ={
      println("looking for:" + inputWord)
      // get word for china
      var ch = word2VecMap.get(inputWord)
      var china = breeze.linalg.DenseVector(0.0f)
      ch match {
        case Some(ni) => china = ni.toDenseVector
        case None => throw new Exception("Could not retrieve word vector for " + inputWord + " not in training set?")
      }
      
      var normChina = breeze.linalg.norm(china,2)
      var cosDists : breeze.linalg.DenseVector[Float] = breeze.linalg.DenseVector.zeros[Float](vocabSize)
      for(i <- 0 to vocabSize - 1){
        cosDists(i) =   wordVecs(::,i).t * china
      }

      var resList: ListBuffer[(String,Double,breeze.linalg.DenseVector[Float])] = new ListBuffer[(String,Double,breeze.linalg.DenseVector[Float])]()

      for(i <- 0 to vocabSize -1){
        resList += new Tuple3(vocabGlob(i).word, cosDists(i) / normsLayer0(i)  ,wordVecs(::,i).toDenseVector )
      }
      resList = resList.sortBy(_._2).reverse
      for(i <- 0 to 5){//vocabSize - 1){
        println("" + resList(i)._1 + "," + resList(i)._2)
      }
    }
  }
}