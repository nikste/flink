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

import org.apache.flink.api.scala.FlinkShell
import org.apache.flink.util.TestLogger
import org.junit.{Assert, Test}
import org.slf4j.{LoggerFactory, Logger}

import scala.tools.nsc.interpreter._

class ScalaShellLocalStartupStreamingITCase extends TestLogger {

    private val LOG: Logger = LoggerFactory.getLogger(classOf[ScalaShellLocalStartupStreamingITCase])
    /**
    * tests flink shell with local setup through startup script in bin folder
    */
  @Test
  def testLocalCluster: Unit = {
    val input = """
        val text = env.fromElements("To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,")
        val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.keyBy(0).sum(1)
        val result = counts.print()
        env.execute()
        :q
        """.stripMargin

    val in: BufferedReader = new BufferedReader(new StringReader(input + "\n"))
    val out: StringWriter = new StringWriter
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val oldOut: PrintStream = System.out
    System.setOut(new PrintStream(baos))
    val args: Array[String] = Array("local","-s")

    val jPrintWriter: JPrintWriter = new JPrintWriter(out)
    //start flink scala shell
    FlinkShell.readWriter = (Some(in),Some(jPrintWriter))
    FlinkShell.main(args)

    baos.flush()
    val output: String = baos.toString
    System.setOut(oldOut)

    println(output)
    FlinkShell.cluster match{
      case Some(c) =>
          LOG.info("cluster:" + c.running)
      case _ =>
          LOG.info("cluster gone!")
    }

    Assert.assertTrue(output.contains("(of,2)"))
    Assert.assertTrue(output.contains("(whether,1)"))
    Assert.assertTrue(output.contains("(to,4)"))
    Assert.assertTrue(output.contains("(arrows,1)"))

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("error"))
    Assert.assertFalse(output.contains("Exception"))
  }
}
