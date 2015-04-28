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


import scala.tools.nsc.Settings

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster

/**
 * Created by owner on 22-4-15.
 */
object FlinkShell {
  def main(args: Array[String]) {
    println("Starting Flink Shell:")

    var cluster: LocalFlinkMiniCluster = null

    val (host, port) = if (args.length < 4) {
      println("No arguments given, starting local cluster.")
      cluster = new LocalFlinkMiniCluster(new Configuration, false)

      ("localhost", cluster.getJobManagerRPCPort)
    } else {
      // more than one argument
      if (args(0) == "-h" || args(0) == "-host" && args(2) == "-p" || args(2) == "-port") {

        val host = args(1)
        val port = args(3).toInt

        println(s"Connecting to remote server (host: $host, port: $port).")

        (host, port)
      } else {
        throw new RuntimeException("Could not parse program arguments.")
      }
    }

    // custom shell
    val repl = new FlinkILoop(host, port) //new MyILoop();

    repl.settings = new Settings()

    // enable this line to use scala in intellij
    repl.settings.usejavacp.value = true

    //repl.createInterpreter()

    // start scala interpreter shell
    repl.process(repl.settings)

    repl.closeInterpreter()

    if (cluster != null) {
      cluster.stop()
    }

    print(" good bye ..")
  }
}
