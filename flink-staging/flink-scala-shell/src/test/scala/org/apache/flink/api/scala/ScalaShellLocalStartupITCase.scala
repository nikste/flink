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

import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

import scala.tools.nsc.interpreter.JPrintWriter


@RunWith(classOf[JUnitRunner])
class ScalaShellLocalStartupITCase extends FunSuite with Matchers {

  /**
   * tests flink shell with local setup through startup script in bin folder
   */
  test("start flink scala shell with local cluster") {

    val input: String = "val els = env.fromElements(\"a\",\"b\");\n" + "els.print\nError\n:q\n"
    val in: BufferedReader = new BufferedReader(new StringReader(input + "\n"))
    val out: StringWriter = new StringWriter
    val jPrintWriter: JPrintWriter = new JPrintWriter(out)

    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val oldOut: PrintStream = System.out
    System.setOut(new PrintStream(baos))
    val args: Array[String] = Array("local")

    //start flink scala shell
    FlinkShell.readWriter = (Some(in),Some(jPrintWriter));
    FlinkShell.main(args)

    baos.flush()
    val output: String = baos.toString
    System.setOut(oldOut)

    output should include("Job execution switched to status FINISHED.")
    output should include("a\nb")

    output should not include "Error"
    output should not include "ERROR"
    output should not include "Exception"
    output should not include "failed"
  }

  /**
   * tests shell in local setup with streaming
   */
  test("WordCount in Shell Streaming") {
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
    /*
      val in: BufferedReader = new BufferedReader(new StringReader(input + "\n"))
      val out: StringWriter = new StringWriter
      val jPrintWriter: JPrintWriter = new JPrintWriter(out)

      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      val oldOut: PrintStream = System.out
      //System.setOut(new PrintStream(baos))
      val args: Array[String] = Array("local","-s")

      //start flink scala shell
      //val outwriter = new PrintWriter(out)
      FlinkShell.readWriter = (Some(in),Some(jPrintWriter))
      FlinkShell.main(args)

      baos.flush()
      val output: String = baos.toString + out.toString
      //System.setOut(oldOut)

      System.out.println("printing output")

      System.out.println(output)


      println(" ")
      println("testing:")
      //output should include("(of,2)")

      output should include("(whether,1)")
      output should include("(to,4)")
      output should include("(arrows,1)")

      output should not include "failed"
      output should not include "error"
      output should not include "Exception"*/

    val in: BufferedReader = new BufferedReader(
      new StringReader(
        input + "\n"))
    val out: StringWriter = new StringWriter
    val jPrintWriter: JPrintWriter = new JPrintWriter(out);

    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val oldOut: PrintStream = System.out
    System.setOut(new PrintStream(baos))

    val args: Array[String] = Array[String]("local","-s")

    //start scala shell with initialized
    // buffered reader for testing
    FlinkShell.readWriter = (Some(in),Some(jPrintWriter))
    FlinkShell.main(args)
    baos.flush()

    val output: String = baos.toString

    System.setOut(oldOut)

    output should include("(of,2)")
    output should include("(whether,1)")
    output should include("(to,4)")
    output should include("(arrows,1)")

    output should not include "failed"
    output should not include "error"
    output should not include "Exception"
  }
}
