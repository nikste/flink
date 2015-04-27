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

import org.apache.flink.api.java.{ScalaShellRemoteEnvironment, JarHelper}
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.util.AbstractID

import scala.tools.nsc.interpreter.ILoop

/**
 * Created by Nikolaas Steenbergen on 16-4-15.
 */
class FlinkILoop(val host:String,val port:Int) extends ILoop {

  // remote environment
  val remoteEnv : ScalaShellRemoteEnvironment = {
    val remoteEnv = new ScalaShellRemoteEnvironment(host,port,this);
    remoteEnv
  }

  // local environment
  val scalaEnv: ExecutionEnvironment = {
    val scalaEnv = new ExecutionEnvironment(remoteEnv);
    scalaEnv
  }

  def this() = this("localhost", new LocalFlinkMiniCluster(new Configuration,false).getJobManagerRPCPort);

  /**
   * creates a temporary directory to store compiled console files
   */
  val tmpDir: File = {
    // get unique temporary folder:
    val abstractID: String = new AbstractID().toString
    val tmpDir: File = new File(System.getProperty("java.io.tmpdir") + "/scala_shell_tmp-" + abstractID)
    if (!tmpDir.exists) {
      tmpDir.mkdir
    }
    tmpDir
  }

  def getTmpDir(): File = {
    return (this.tmpDir);
  }

  /**
   * writes contents of the compiled lines that have been executed in the shell into a "physical directory":
   * creates a unique temporary directory
   */
  def writeFilesToDisk(): Unit = {
    val vd = intp.virtualDirectory

    var vdIt = vd.iterator

    var basePath = tmpDir.getAbsolutePath + "/scala_shell_commands/"

    for (fi <- vdIt) {
      if (fi.isDirectory) {

        var fullPath = basePath + fi.name + "/"

        var fiIt = fi.iterator

        for (f <- fiIt) {

          // create directories
          var newfile = new File(fullPath)
          newfile.mkdirs()
          newfile = new File(fullPath + f.name)

          var outputStream = new FileOutputStream(newfile)
          var inputStream = f.input
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
  override def prompt = "==> "

  addThunk {
    intp.beQuietDuring {
      // automatically imports the flink scala api
      intp.addImports("org.apache.flink.api.scala._")

      // with this we can access this object in the scala shell
      intp.bindValue("intp", this)
      intp.bindValue("env", this.scalaEnv)
    }
  }

  /**
   * custom welcome message
   */
  override def printWelcome() {
    echo("\n" +
    "  ____$$$$$$$$$$\n" +
    "  __$$8888888888$$\n" +
    "  $$$$888888888888$$\n" +
    "  ____$$88888888888$$\n" +
    "  _____ $$88888888888$$\n" +
    "  ______$$8888888888$$\n" +
    "  ______$$8888888888$$__________$$__$$\n" +
    "  ___ ___$$8888888888$$__________$$$$$$\n" +
    "  ______$$88888888$$$$__________$$8888$$\n" +
    "  _____ _$$8888888$$$$__________$$88888888\n" +
    "  ____ $$888888888j$$ _________$$88888( € )88\n" +
    "___$$8888888o$$$$ ________s$$888888888888\n" +
    "  __$$8888888h$$$$ _____s$$$$88$$88888888(®)\n" +
    "__$$8888888a$$_____s$$888888$$888888____s//$\n" +
    "__$$88 888n$$$$ __$$$$8888888888$$88_____$$$$\n" +
    "___$$8888n$$ __$$8888888888888888$$$$$??_$$s\n" +
    "  ____$$88888a$$88888888$$$$888888888888$$\n" +
    "____$ $888888$$888888888888$$$$\n" +
    "  _____$$88888$$888888888888888$$\n" +
    "______$$$$8888$$8888 8888888888$$\n" +
    "  __________$$$$8888888888888888$$\n" +
    "______________$$888888888888$$\n" +
    "_______________$$$$8888888$$\n" +
    "_____________$$$$_$$$$$$$$$$$$$$\n" +
      "\n" +
      "            F L I N K                    \n")
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
}
