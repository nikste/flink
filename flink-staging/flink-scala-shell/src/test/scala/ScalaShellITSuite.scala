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

import java.io.{PrintWriter, BufferedReader, StringReader, StringWriter}

import org.apache.flink.api.scala.FlinkILoop
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
 * Created by Nikolaas Steenbergen on 28-4-15.
 */



@RunWith(classOf[Parameterized])
class ScalaShellITSuite(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode){


  /**
   * initializes new local cluster and processes commands given in input
   * @param input commands to be processed in the shell
   * @return output of shell
   */
  def processInShell(input : String): String ={
    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()

    // new local cluster
    val cluster = new LocalFlinkMiniCluster(new Configuration, false)
    val host = "localhost"
    val port = cluster.getJobManagerRPCPort

    val flinkILoop = new FlinkILoop(host, port, in,new PrintWriter(out))

    // return result of input
    return out.toString
  }
  /**
   * test the creation of a local environment (if not given any connection parameters: host and port)
   */
  @Test
  def testLocalRemoteEnvironment(): Unit = {

    val input : String =
      """
        |val text = env.fromElements("To be, or not to be,--that is the question:--","Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune","Or to take arms against a sea of troubles,")
        |val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.groupBy(0).sum(1)
        |counts.print()
      """

      val output : String = processInShell(input)
      println(output)
    }
}
