/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.scala


import scala.tools.nsc.Settings

/**
 * Created by owner on 22-4-15.
 */
object FlinkShell {
  def main(args: Array[String]) {
	println("Starting Flink Shell:")

	if(args.length < 4){
		println("No arguments given, initializing as local Environment")
	}
	else { // more than one argument
	  if (args(0) == "-h" || args(0) == "-host" && args(2) == "-p" || args(2) == "-port") {

		val host = args(1)
		val port = args(3).toInt

		println("starting remote server with parameters:")
		println("host:" + host)
		println("port:" + port)
	  }
	}

	// custom shell
	val repl = new FlinkILoop //new MyILoop();

	repl.settings = new Settings()

	// enable this line to use scala in intellij
	repl.settings.usejavacp.value = true

	repl.createInterpreter()

	// start scala interpreter shell
	repl.process(repl.settings)

	repl.closeInterpreter()

	print(" good bye ..")
  }
}
