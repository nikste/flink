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

import java.io.{File, FileOutputStream}

import scala.tools.nsc.interpreter.ILoop

import org.apache.flink.api.java.ScalaShellRemoteEnvironment
import org.apache.flink.util.AbstractID

/**
 * Created by Nikolaas Steenbergen on 16-4-15.
 */
class FlinkILoop(val host:String, val port:Int) extends ILoop {
  
  // remote environment
  private val remoteEnv : ScalaShellRemoteEnvironment = {
    val remoteEnv = new ScalaShellRemoteEnvironment(host,port,this)
    remoteEnv
  }

  // local environment
  private val scalaEnv: ExecutionEnvironment = {
    val scalaEnv = new ExecutionEnvironment(remoteEnv)
    scalaEnv
  }

  /**
   * creates a temporary directory to store compiled console files
   */
  private val tmpDirBase: File = {
    // get unique temporary folder:
    val abstractID: String = new AbstractID().toString
    val tmpDir: File = new File(System.getProperty("java.io.tmpdir") , "scala_shell_tmp-" + abstractID)
    if (!tmpDir.exists) {
      tmpDir.mkdir
    }
    tmpDir
  }

  // scala_shell commands
  private val tmpDirShell : File = {
    new File(tmpDirBase,"scala_shell_commands")
  }

  // scala shell jar file name
  private val tmpJarShell : File = {
    new File(tmpDirBase,"scala_shell_commands.jar")
  }


  /**
   * writes contents of the compiled lines that have been executed in the shell into a "physical directory":
   * creates a unique temporary directory
   */
  def writeFilesToDisk(): Unit = {
    val vd = intp.virtualDirectory

    var vdIt = vd.iterator

    for (fi <- vdIt) {
      if (fi.isDirectory) {

        var fiIt = fi.iterator

        for (f <- fiIt) {

          // directory for compiled line
          val lineDir = new File(tmpDirShell.getAbsolutePath, fi.name)
          lineDir.mkdirs()

          // compiled classes for commands from shell
          val writeFile = new File(lineDir.getAbsolutePath,f.name )
          val outputStream = new FileOutputStream(writeFile)
          val inputStream = f.input

          // copy file contents
          org.apache.commons.io.IOUtils.copy(inputStream, outputStream)

          inputStream.close()
          outputStream.close()
        }
      }
    }
  }

  /**
   * CUSTOM START METHODS OVERRIDE:
   */
  override def prompt = "Scala-Flink> "

  addThunk {
    intp.beQuietDuring {
      // automatically imports the flink scala api
      intp.addImports("org.apache.flink.api.scala._")

      // with this we can access this object in the scala shell
      intp.bindValue("env", this.scalaEnv)
    }
  }

  /**
   * custom welcome message
   */
  override def printWelcome() {
    echo(
    """
    $$$$$$$$$$
  $$8888888888$$
$$$$888888888888$$
    $$88888888888$$
     $$88888888888$$
      $$8888888888$$
      $$8888888888$$          $$  $$
      $$8888888888$$          $$$$$$
      $$88888888$$$$          $$8888$$
      $$8888888$$$$          $$88888888
    $$888888888j$$         $$88888( € )88
   $$8888888o$$$$        s$$888888888888
  $$8888888h$$$$     s$$$$88$$88888888(®)
  $$8888888a$$     s$$888888$$888888    s//$
  $$88888n$$$$  $$$$8888888888$$88     $$$$
   $$8888n$$  $$8888888888888888$$$$$?? $$s
    $$88888a$$88888888$$$$888888888888$$
    $$888888$$888888888888$$$$
     $$88888$$888888888888888$$
      $$$$8888$$88888888888888$$
          $$$$8888888888888888$$
              $$888888888888$$
               $$$$8888888$$
             $$$$ $$$$$$$$$$$$$$

            F L I N K

NOTE: Use the prebound Execution Environment "env" to read data and execute your program:
  * env.readTextFile("/path/to/data")
  * env.execute("Program name")

HINT: You can use print() on a DataSet to print the contents to this shell.
       """)
    

  }


  /**
   * We override this for custom Flink commands
   * The main read-eval-print loop for the repl.  It calls
   * command() for each line of input, and stops when
   * command() returns false.
   */
  override def loop() {
    def readOneLine() = {
      out.flush()
      in readLine prompt
    }
    // return false if repl should exit
    def processLine(line: String): Boolean = {
      if (isAsync) {
        if (!awaitInitialized()) return false
        runThunks()
      }
      // example custom catch Flink phrase:
      /*
      if (line == "writeFlinkVD") {
        writeFilesToDisk()
        return (true)
      }
      */

      if (line eq null) false // assume null means EOF
      else command(line) match {
        case Result(false, _) => false
        case Result(_, Some(finalLine)) => addReplay(finalLine); true
        case _ => true
      }
    }
    def innerLoop() {
      if (try processLine(readOneLine()) catch crashRecovery)
        innerLoop()
    }
    innerLoop()
  }

  /**
   * needs to be redeclared because its declared private by the parent.
   */
  private val crashRecovery: PartialFunction[Throwable, Boolean] = {
    case ex: Throwable =>
      echo(intp.global.throwableAsString(ex))

      ex match {
        case _: NoSuchMethodError | _: NoClassDefFoundError =>
          echo("\nUnrecoverable error.")
          throw ex
        case _ =>
          def fn(): Boolean =
            try in.readYesOrNo(replayQuestionMessage, {
              echo("\nYou must enter y or n."); fn()
            })
            catch {
              case _: RuntimeException => false
            }

          if (fn()) replay()
          else echo("\nAbandoning crashed session.")
      }
      true
  }
  
  //  getter functions:
  // get (root temporary folder)
  def getTmpDirBase(): File = {
    return (this.tmpDirBase);
  }
  
  // get shell folder name inside tmp dir
  def getTmpDirShell(): File = {
    return (this.tmpDirShell)
  }

  // get tmp jar file name
  def getTmpJarShell(): File = {
    return (this.tmpJarShell)
  }
}
