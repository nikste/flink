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

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster

import scala.tools.nsc.Settings

/**
 * Created by nikste on 8/24/15.
 */
object testStreamingCollect {
  var cluster = new LocalFlinkMiniCluster(new Configuration, false, StreamingMode.STREAMING)
  //var flinkIloop =
  // new FlinkILoop("localhost",
  // cluster.getJobManagerRPCPort,
  // StreamingMode.STREAMING,None,
  // inputBufferedReader,
  // outInterpreterJPrintWrtier)

  /**
   * Run the input using a Scala Shell and return the output of the shell.
   * @param input commands to be processed in the shell
   * @return output of shell
   */
  def processInShell(input : String): String ={

    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val baos = new ByteArrayOutputStream()

    val oldOut = System.out
    System.setOut(new PrintStream(baos))

    // new local cluster
    val host = "localhost"
    val port = cluster.getJobManagerRPCPort

    var repl : FlinkILoop=  new FlinkILoop(
      host,port, StreamingMode.STREAMING,
      in,new PrintWriter(out))


    repl.settings = new Settings()

    // enable this line to use scala in intellij
    repl.settings.usejavacp.value = true

    repl.process(repl.settings)

    repl.closeInterpreter()

    System.setOut(oldOut)

    baos.flush()

    val stdout = baos.toString

    //out.toString //+ stdout
    stdout + out.toString
  }
  def main(args:Array[String]) : Unit ={
    println("start")

    var commands =
      """
    println("starting test in shell")

    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+").filter{ _.nonEmpty } }.map { (_, 1) }.groupBy(0).sum(1)

    import org.apache.flink.contrib.streaming.scala.DataStreamUtils._
    var collected = collect[(String,Int)](counts)


    while(collected.hasNext()){
      var el = collected.next()
      println("next element:" + el)
      }
    println("ending test in shell")
      """.stripMargin
    println(processInShell(commands))

    println("end")
  }
}
