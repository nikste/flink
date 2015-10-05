package org.apache.flink.ml.feature_extraction

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}
import org.apache.flink.api.scala._
/**
 * Created by nikste on 05.10.15.
 */
class Word2VecTestSuite  extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The Word2Vec implementation"

  it should "train a word2vec model on four sentences" in {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()

    //var t : DataSet[Array[String]] = env.readTextFile("/media/nikste/moarspace/workspace-flink/datasets/text8")
    //  .map(line => line.split(" "))
    var inputDataSeq : DataSet[Array[String]] =
      env.fromElements[Array[String]](Array("a","b","a","b","a","b"),
        Array("b","c","b","c","b","c"),
        Array("a","c","a","c","a","c"),
        Array("c","d","c","d","c","d"),
        Array("a","d","a","d","a","d"),
        Array("e","f","e","f","e","f"))
    /*readTextFile("/media/nikste/moarspace/workspace-flink/datasets/text8")
        .map(line => line.split(" "))*/

    val w2v = Word2Vec()

    w2v.setNumIterations(100000)
    w2v.setBatchSize(1)
    w2v.setVectorSize(2)
    w2v.fit(inputDataSeq)
    var vocab = Word2Vec.getVocab()

    // b same context as c,d
    println("a")
    Word2Vec.printFirstN(Word2Vec.findSynonyms("a"),5)
    // b same context as c,d
    println("b")
    Word2Vec.printFirstN(Word2Vec.findSynonyms("b"),5)
    // d same context as b,c
    println("d")
    Word2Vec.printFirstN(Word2Vec.findSynonyms("d"),5)
    // e same context as f
    println("e")
    Word2Vec.printFirstN(Word2Vec.findSynonyms("e"),5)

  }
}
