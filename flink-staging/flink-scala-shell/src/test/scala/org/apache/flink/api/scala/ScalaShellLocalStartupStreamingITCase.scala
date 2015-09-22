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
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}


@RunWith(classOf[JUnitRunner])
class ScalaShellLocalStartupStreamingITCase extends FunSuite with Matchers {

  /**
   * tests flink shell with local setup in Streaming mode
   */
  test("start flink scala shell with local cluster streaming mode") {

    val input: String = "val els = env.fromElements(\"a\",\"b\");\n" +
      "els.print\nError\nenv.execute()\n:q\n"

    val in: BufferedReader = new BufferedReader(new StringReader(input + "\n"))
    val out: StringWriter = new StringWriter
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val oldOut: PrintStream = System.out
    System.setOut(new PrintStream(baos))

    val args: Array[String] = Array("local","-s")

    //start flink scala shell
    FlinkShell.bufferedReader = in;
    FlinkShell.main(args)


    baos.flush
    val output: String = baos.toString
    System.setOut(oldOut)

    assert(output.contains("Job execution switched to status FINISHED."))
    assert(output.contains("a\nb"))

    assert((!output.contains("Error")))
    assert((!output.contains("ERROR")))
    assert((!output.contains("Exception")))
    assert((!output.contains("failed")))
  }
}
