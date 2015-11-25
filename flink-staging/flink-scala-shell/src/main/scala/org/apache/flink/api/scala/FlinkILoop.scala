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

import java.io.{BufferedReader, File, FileOutputStream}

import org.apache.flink.api.java.{ScalaShellRemoteStreamEnvironment, JarHelper, ScalaShellRemoteEnvironment}
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.AbstractID

import scala.tools.nsc.interpreter._


class FlinkILoop(
    val host: String,
    val port: Int,
    val streaming: StreamingMode,
    val externalJars: Option[Array[String]],
    in0: Option[BufferedReader],
    out0: JPrintWriter)
  extends ILoopCompat(in0, out0) {

  def this(host: String,
           port: Int,
           streaming: StreamingMode,
           externalJars: Option[Array[String]],
           in0: BufferedReader, 
           out: JPrintWriter){
    this(
      host,
      port,
      streaming,
      externalJars,
      Some(in0),
      out)
  }

  def this(host: String,
           port: Int,
           streaming: StreamingMode,
           externalJars: Option[Array[String]]){
    this(host,
      port,
      streaming,
      externalJars,
      None,
      new JPrintWriter(Console.out, true))
  }
  
  def this(host: String,
           port: Int,
           streaming: StreamingMode,
           in0: BufferedReader,
           out: JPrintWriter){
    this(host,
      port,
      streaming,
      None,
      in0,
      out)
  }

  // remote environment
  private val remoteEnv  = {
    if(streaming == StreamingMode.STREAMING) {
    // allow creation of environments
    ScalaShellRemoteStreamEnvironment.resetContextEnvironments()
    
    // create our environment that submits against the cluster (local or remote)
    val remoteEnv = new ScalaShellRemoteStreamEnvironment(host, port, this)

    // prevent further instantiation of environments
    ScalaShellRemoteStreamEnvironment.disableAllContextAndOtherEnvironments()
    
    remoteEnv
    }else{
    
    // allow creation of environments
    ScalaShellRemoteEnvironment.resetContextEnvironments()
    
    // create our environment that submits against the cluster (local or remote)
    val remoteEnv = new ScalaShellRemoteEnvironment(host, port, this)
    
    // prevent further instantiation of environments
    ScalaShellRemoteEnvironment.disableAllContextAndOtherEnvironments()
    
    remoteEnv
    }
  }

  // local environment
  val scalaEnv = {
    remoteEnv match{
      case s: ScalaShellRemoteStreamEnvironment =>
        new StreamExecutionEnvironment(s)
      case b: ScalaShellRemoteEnvironment =>
        new ExecutionEnvironment(b)
    }
  }

  /**
   * creates a temporary directory to store compiled console files
   */
  private val tmpDirBase: File = {
    // get unique temporary folder:
    val abstractID: String = new AbstractID().toString
    val tmpDir: File = new File(
      System.getProperty("java.io.tmpdir"),
      "scala_shell_tmp-" + abstractID)
    if (!tmpDir.exists) {
      tmpDir.mkdir
    }
    tmpDir
  }

  // scala_shell commands
  private val tmpDirShell: File = {
    new File(tmpDirBase, "scala_shell_commands")
  }

  // scala shell jar file name
  private val tmpJarShell: File = {
    new File(tmpDirBase, "scala_shell_commands.jar")
  }

  private val packageImports =
    streaming match {
      case StreamingMode.BATCH_ONLY => Seq[String](
      "org.apache.flink.core.fs._",
      "org.apache.flink.core.fs.local._",
      "org.apache.flink.api.common.io._",
      "org.apache.flink.api.common.aggregators._",
      "org.apache.flink.api.common.accumulators._",
      "org.apache.flink.api.common.distributions._",
      "org.apache.flink.api.common.operators._",
      "org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint",
      "org.apache.flink.api.common.functions._",
      "org.apache.flink.api.java.io._",
      "org.apache.flink.api.java.aggregation._",
      "org.apache.flink.api.java.functions._",
      "org.apache.flink.api.java.operators._",
      "org.apache.flink.api.java.sampling._",
      "org.apache.flink.api.scala._",
      "org.apache.flink.api.scala.utils._"
    )
      case StreamingMode.STREAMING => Seq[String](
       "org.apache.flink.core.fs._",
      "org.apache.flink.core.fs.local._",
      "org.apache.flink.api.common.io._",
      "org.apache.flink.api.common.aggregators._",
      "org.apache.flink.api.common.accumulators._",
      "org.apache.flink.api.common.distributions._",
      "org.apache.flink.api.common.operators._",
      "org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint",
      "org.apache.flink.api.common.functions._",
      "org.apache.flink.api.java.io._",
      "org.apache.flink.api.java.aggregation._",
      "org.apache.flink.api.java.functions._",
      "org.apache.flink.api.java.operators._",
      "org.apache.flink.api.java.sampling._",
      "org.apache.flink.api.scala._",
      "org.apache.flink.api.scala.utils._",
      "org.apache.flink.streaming._",
      "org.apache.flink.streaming.connectors.rabbitmq._",
      "org.apache.flink.contrib.streaming.scala.DataStreamUtils._",
      "org.apache.flink.streaming.util.serialization.SimpleStringSchema;"
      )
    }

  override def createInterpreter(): Unit = {
    super.createInterpreter()

    addThunk {
      intp.beQuietDuring {
        // import dependencies
        intp.interpret("import " + packageImports.mkString(", "))
        streaming match {
          case StreamingMode.STREAMING =>
            intp.bind("env", this.scalaEnv.asInstanceOf[StreamExecutionEnvironment])
          case StreamingMode.BATCH_ONLY =>
            intp.bind("env", this.scalaEnv.asInstanceOf[ExecutionEnvironment])
        }
      }
    }
  }

  def resetInterpreter(): Unit = {
    intp.virtualDirectory.clear()
    addThunk()
  }

  /**
   * Packages the compiled classes of the current shell session into a Jar file for execution
   * on a Flink cluster.
   *
   * @return The path of the created Jar file
   */
  def writeFilesToDisk(): File = {
    val vd = intp.virtualDirectory

    val vdIt = vd.iterator

    for (fi <- vdIt) {
      if (fi.isDirectory) {

        val fiIt = fi.iterator

        for (f <- fiIt) {

          // directory for compiled line
          val lineDir = new File(tmpDirShell.getAbsolutePath, fi.name)
          lineDir.mkdirs()

          // compiled classes for commands from shell
          val writeFile = new File(lineDir.getAbsolutePath, f.name)
          val outputStream = new FileOutputStream(writeFile)
          val inputStream = f.input

          // copy file contents
          org.apache.commons.io.IOUtils.copy(inputStream, outputStream)

          inputStream.close()
          outputStream.close()
        }
      }
    }

    val compiledClasses = new File(tmpDirShell.getAbsolutePath)

    val jarFilePath = new File(tmpJarShell.getAbsolutePath)

    val jh: JarHelper = new JarHelper
    jh.jarDir(compiledClasses, jarFilePath)

    jarFilePath
  }

  /**
   * custom welcome message
   */
  override def printWelcome() {
    echo(
      // scalastyle:off
      """
                         \u2592\u2593\u2588\u2588\u2593\u2588\u2588\u2592
                     \u2593\u2588\u2588\u2588\u2588\u2592\u2592\u2588\u2593\u2592\u2593\u2588\u2588\u2588\u2593\u2592
                  \u2593\u2588\u2588\u2588\u2593\u2591\u2591        \u2592\u2592\u2592\u2593\u2588\u2588\u2592  \u2592
                \u2591\u2588\u2588\u2592   \u2592\u2592\u2593\u2593\u2588\u2593\u2593\u2592\u2591      \u2592\u2588\u2588\u2588\u2588
                \u2588\u2588\u2592         \u2591\u2592\u2593\u2588\u2588\u2588\u2592    \u2592\u2588\u2592\u2588\u2592
                  \u2591\u2593\u2588            \u2588\u2588\u2588   \u2593\u2591\u2592\u2588\u2588
                    \u2593\u2588       \u2592\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591\u2592\u2591\u2593\u2593\u2588
                  \u2588\u2591 \u2588   \u2592\u2592\u2591       \u2588\u2588\u2588\u2593\u2593\u2588 \u2592\u2588\u2592\u2592\u2592
                  \u2588\u2588\u2588\u2588\u2591   \u2592\u2593\u2588\u2593      \u2588\u2588\u2592\u2592\u2592 \u2593\u2588\u2588\u2588\u2592
               \u2591\u2592\u2588\u2593\u2593\u2588\u2588       \u2593\u2588\u2592    \u2593\u2588\u2592\u2593\u2588\u2588\u2593 \u2591\u2588\u2591
         \u2593\u2591\u2592\u2593\u2588\u2588\u2588\u2588\u2592 \u2588\u2588         \u2592\u2588    \u2588\u2593\u2591\u2592\u2588\u2592\u2591\u2592\u2588\u2592
        \u2588\u2588\u2588\u2593\u2591\u2588\u2588\u2593  \u2593\u2588           \u2588   \u2588\u2593 \u2592\u2593\u2588\u2593\u2593\u2588\u2592
      \u2591\u2588\u2588\u2593  \u2591\u2588\u2591            \u2588  \u2588\u2592 \u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2592 \u2588\u2588\u2593\u2591\u2592
     \u2588\u2588\u2588\u2591 \u2591 \u2588\u2591          \u2593 \u2591\u2588 \u2588\u2588\u2588\u2588\u2588\u2592\u2591\u2591    \u2591\u2588\u2591\u2593  \u2593\u2591
    \u2588\u2588\u2593\u2588 \u2592\u2592\u2593\u2592          \u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2591       \u2592\u2588\u2592 \u2592\u2593 \u2593\u2588\u2588\u2593
 \u2592\u2588\u2588\u2593 \u2593\u2588 \u2588\u2593\u2588       \u2591\u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2593\u2592\u2591         \u2588\u2588\u2592\u2592  \u2588 \u2592  \u2593\u2588\u2592
 \u2593\u2588\u2593  \u2593\u2588 \u2588\u2588\u2593 \u2591\u2593\u2593\u2593\u2593\u2593\u2593\u2593\u2592              \u2592\u2588\u2588\u2593           \u2591\u2588\u2592
 \u2593\u2588    \u2588 \u2593\u2588\u2588\u2588\u2593\u2592\u2591              \u2591\u2593\u2593\u2593\u2588\u2588\u2588\u2593          \u2591\u2592\u2591 \u2593\u2588
 \u2588\u2588\u2593    \u2588\u2588\u2592    \u2591\u2592\u2593\u2593\u2588\u2588\u2588\u2593\u2593\u2593\u2593\u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2592            \u2593\u2588\u2588\u2588  \u2588
\u2593\u2588\u2588\u2588\u2592 \u2588\u2588\u2588   \u2591\u2593\u2593\u2592\u2591\u2591   \u2591\u2593\u2588\u2588\u2588\u2588\u2593\u2591                  \u2591\u2592\u2593\u2592  \u2588\u2593
\u2588\u2593\u2592\u2592\u2593\u2593\u2588\u2588  \u2591\u2592\u2592\u2591\u2591\u2591\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591                            \u2588\u2593
\u2588\u2588 \u2593\u2591\u2592\u2588   \u2593\u2593\u2593\u2593\u2592\u2591\u2591  \u2592\u2588\u2593       \u2592\u2593\u2593\u2588\u2588\u2593    \u2593\u2592          \u2592\u2592\u2593
\u2593\u2588\u2593 \u2593\u2592\u2588  \u2588\u2593\u2591  \u2591\u2592\u2593\u2593\u2588\u2588\u2592            \u2591\u2593\u2588\u2592   \u2592\u2592\u2592\u2591\u2592\u2592\u2593\u2588\u2588\u2588\u2588\u2588\u2592
 \u2588\u2588\u2591 \u2593\u2588\u2592\u2588\u2592  \u2592\u2593\u2593\u2592  \u2593\u2588                \u2588\u2591      \u2591\u2591\u2591\u2591   \u2591\u2588\u2592
 \u2593\u2588   \u2592\u2588\u2593   \u2591     \u2588\u2591                \u2592\u2588              \u2588\u2593
  \u2588\u2593   \u2588\u2588         \u2588\u2591                 \u2593\u2593        \u2592\u2588\u2593\u2593\u2593\u2592\u2588\u2591
   \u2588\u2593 \u2591\u2593\u2588\u2588\u2591       \u2593\u2592                  \u2593\u2588\u2593\u2592\u2591\u2591\u2591\u2592\u2593\u2588\u2591    \u2592\u2588
    \u2588\u2588   \u2593\u2588\u2593\u2591      \u2592                    \u2591\u2592\u2588\u2592\u2588\u2588\u2592      \u2593\u2593
     \u2593\u2588\u2592   \u2592\u2588\u2593\u2592\u2591                         \u2592\u2592 \u2588\u2592\u2588\u2593\u2592\u2592\u2591\u2591\u2592\u2588\u2588
      \u2591\u2588\u2588\u2592    \u2592\u2593\u2593\u2592                     \u2593\u2588\u2588\u2593\u2592\u2588\u2592 \u2591\u2593\u2593\u2593\u2593\u2592\u2588\u2593
        \u2591\u2593\u2588\u2588\u2592                          \u2593\u2591  \u2592\u2588\u2593\u2588  \u2591\u2591\u2592\u2592\u2592
            \u2592\u2593\u2593\u2593\u2593\u2593\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2591\u2591\u2593\u2593  \u2593\u2591\u2592\u2588\u2591

              F L I N K - S C A L A - S H E L L

NOTE: Use the prebound Execution Environment "env" to read data and execute your program:
  * env.readTextFile("/path/to/data")
  * env.execute("Program name")

HINT: You can use print() on a DataSet to print the contents to this shell.
      """
    // scalastyle:on
    )

  }

  def getExternalJars(): Array[String] = externalJars.getOrElse(Array.empty[String])
}

