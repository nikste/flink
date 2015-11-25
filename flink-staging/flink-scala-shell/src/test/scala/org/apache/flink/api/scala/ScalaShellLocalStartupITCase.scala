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

import org.apache.flink.util.TestLogger
import org.junit.Test
import org.junit.Assert
import org.slf4j.{LoggerFactory, Logger}

import scala.tools.nsc.interpreter._

class ScalaShellLocalStartupITCase extends TestLogger {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ScalaShellLocalStartupITCase])
  /**
   * tests flink shell with local setup through startup script in bin folder
   */
  @Test
  def testLocalClusterBatch: Unit = {
    // Calling interpret sets the current thread's context classloader,
    // but fails to reset it to its previous state when returning from that method.
    // see: https://issues.scala-lang.org/browse/SI-8521
    val cl = Thread.currentThread().getContextClassLoader
    val input: String =
      """
        import org.apache.flink.api.common.functions.RichMapFunction
        import org.apache.flink.api.java.io.PrintingOutputFormat
        import org.apache.flink.api.common.accumulators.IntCounter
        import org.apache.flink.configuration.Configuration

        val els = env.fromElements("foobar","barfoo")
        val mapped = els.map{
         new RichMapFunction[String, String]() {
            var intCounter: IntCounter = _
            override def open(conf: Configuration): Unit = {
            intCounter = getRuntimeContext.getIntCounter("intCounter")
           }

           def map(element: String): String = {
             intCounter.add(1)
             element
           }
         }
        }
        mapped.output(new PrintingOutputFormat())
        val executionResult = env.execute("Test Job")
        System.out.println("IntCounter: " + executionResult.getIntCounterResult("intCounter"))

        :q
      """.stripMargin

    val in: BufferedReader = new BufferedReader(new StringReader(input + "\n"))
    val out: StringWriter = new StringWriter
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val oldOut: PrintStream = System.out
    System.setOut(new PrintStream(baos))
    val args: Array[String] = Array("local")

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
    Assert.assertTrue(output.contains("IntCounter: 2"))
    Assert.assertTrue(output.contains("foobar"))
    Assert.assertTrue(output.contains("barfoo"))

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("Error"))
    Assert.assertFalse(output.contains("ERROR"))
    Assert.assertFalse(output.contains("Exception"))

    //reset classloader
    Thread.currentThread().setContextClassLoader(cl)
  }

  /**
    * tests flink shell with local setup through startup script in bin folder
    */
  @Test
  def testLocalClusterStreaming: Unit = {
    // Calling interpret sets the current thread's context classloader,
    // but fails to reset it to its previous state when returning from that method.
    // see: https://issues.scala-lang.org/browse/SI-8521
    val cl = Thread.currentThread().getContextClassLoader
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

    // reset classloader
    Thread.currentThread().setContextClassLoader(cl)
  }
}
