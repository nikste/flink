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


import java.io.{StringWriter, BufferedReader}

import org.apache.flink.runtime.StreamingMode

import scala.tools.nsc.Settings

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster

import scala.tools.nsc.interpreter._


object FlinkShell {

  val LOCAL = 0;
  val REMOTE = 1;
  val UNDEFINED = -1;

  var bufferedReader: BufferedReader = null;

  def main(args: Array[String]) {

    // scopt, command line arguments
    case class Config(
        port: Int = -1,
        host: String = "none",
        externalJars: Option[Array[String]] = None,
        flinkShellExecutionMode : Int = UNDEFINED,
        streamingMode : StreamingMode = StreamingMode.BATCH_ONLY)

    val parser = new scopt.OptionParser[Config]("start-scala-shell.sh") {
      head ("Flink Scala Shell")

      cmd("local") action {
        (_,c) => c.copy( host = "none", port = -1, flinkShellExecutionMode = LOCAL)
      } text("starts Flink scala shell with a local Flink cluster\n") children(
        opt[(String)] ("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x,c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
          } text("specifies additional jars to be used in Flink\n"),
        opt[Unit]("streaming") abbr("s") action {
          case (_,c) =>
            c.copy(streamingMode = StreamingMode.STREAMING)
        } text("set this to use streaming api")
        )

      cmd("remote") action { (_, c) =>
        c.copy(flinkShellExecutionMode = REMOTE)
      } text("starts Flink scala shell connecting to a remote cluster\n") children(
        arg[String]("<host>") action { (h, c) =>
          c.copy(host = h) }
          text("remote host name as string"),
        arg[Int]("<port>") action { (p, c) =>
          c.copy(port = p) }
          text("remote port as integer\n"),
        opt[(String)] ("addclasspath") abbr("a")  valueName("<path/to/jar>") action {
          case (x,c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
          } text("specifies additional jars to be used in Flink"),
        opt[Unit]("streaming") abbr("s") action {
        case (_,c) =>
          c.copy(streamingMode = StreamingMode.STREAMING)
      } text("set this to use streaming api")

      )
      help("help") abbr("h") text("prints this usage text\n")
    }


    // parse arguments
    parser.parse (args, Config () ) match {
      case Some(config) =>
        startShell(config.host,
          config.port,
          config.flinkShellExecutionMode,
          config.externalJars,
          config.streamingMode)

      case _ => println("Could not parse program arguments")
    }
  }


  def startShell(
      userHost : String, 
      userPort : Int,
      executionMode : Int,
      externalJars : Option[Array[String]] = None,
      streamingMode : StreamingMode = StreamingMode.BATCH_ONLY
                  ): Unit ={
    
    println("Starting Flink Shell:")

    var cluster: LocalFlinkMiniCluster = null

    // either port or userhost not specified by user, create new minicluster
    val (host,port) = if (executionMode == LOCAL) {
      cluster = new LocalFlinkMiniCluster(new Configuration, false, streamingMode)
      cluster.start()
      val port = cluster.getLeaderRPCPort
      println(s"\nStarting local Flink cluster (host: localhost, port: $port).\n")
      ("localhost",port)
    } else if(executionMode == UNDEFINED) {
      println("Error: please specify execution mode:")
      println("local | remote <host> <port> [-s | -streaming]")
      return
    } else if(userHost == "none" || userPort == -1){
      println("Error: <host> or <port> not specified!")
      return
    } else {
      println(s"\nConnecting to Flink cluster (host: $userHost, port: $userPort).\n")
      (userHost, userPort)
    }

    
    // custom shell
    var repl : FlinkILoop = null;
    if(bufferedReader == null) {
      repl = new FlinkILoop(host,
        port,
        streamingMode,
        externalJars) //new MyILoop();
    }else{
      val out = new StringWriter()
      repl = new FlinkILoop(host,
        port,
        streamingMode,
        externalJars,
        bufferedReader,
        new  JPrintWriter(out));
    }
    repl.settings = new Settings()

    repl.settings.usejavacp.value = true

    // start scala interpreter shell
    repl.process(repl.settings)

    //repl.closeInterpreter()

    if (cluster != null) {
      cluster.stop()
      cluster = null;
    }

    println(" good bye ..")
  }
}
