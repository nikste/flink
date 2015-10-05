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
