package org.apache.flink.api.scala

import java.io.{IOException, ObjectInputStream}

import org.apache.flink.api.common.functions.MapFunction

import scala.util.Random

/**
 * Created by nikste on 6/24/15.
 */
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
