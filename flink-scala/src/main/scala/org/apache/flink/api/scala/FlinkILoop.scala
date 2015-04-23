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

import org.apache.flink.api.java.JarHelper
import org.apache.flink.api.scala.shell.ScalaShellRemoteEnvironment
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.configuration.{ConfigConstants, Configuration}

import scala.tools.nsc.interpreter.ILoop

/**
 * Created by Nikolaas Steenbergen on 16-4-15.
 */
class FlinkILoop extends ILoop {



  // flink local cluster
  val cluster  = new LocalFlinkMiniCluster(new Configuration,false) // port offen, jobs annehmen (local actor system for akka)

  // port
  val clusterPort = cluster.getJobManagerRPCPort

  // remote environment
  var remoteEnv = new ScalaShellRemoteEnvironment("localhost", clusterPort, this);//new RemoteEnvironment("localhost", clusterPort)

  val scalaEnv = new ExecutionEnvironment(remoteEnv)


  /**
   * writes contents of the compiled lines that have been executed in the shell into a "physical directory":
   * /tmp/scala_shell/
   */
  def writeFilesToDisk(): Unit = {
    val vd = intp.virtualDirectory

    var vdIt = vd.iterator

    var basePath = "/tmp/scala_shell/"

    for (fi <- vdIt) {
      if (fi.isDirectory) {

        var fullPath = basePath + fi.name + "/"

        var fiIt = fi.iterator

        for (f <- fiIt) {

          //println(f.path + f.name + ": isDir:" + f.isDirectory)

          // create directories
          //var fullPath = basePath + z + "/"
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
    println("I've written all files to diks for you.")
  }

  /**
   * creates jar file to /tmp/scala_shell.jar from contents of virtual directory
   * this is happening here because maven forbids circular dependencies.
   */
  def createJarFile(): Unit ={

    // first write files
    writeFilesToDisk()


    // then package them to a jar
    val  jh = new JarHelper

    val inFile = new File("/tmp/scala_shell");
    val outFile = new File("/tmp/scala_shell.jar")


    jh.jarDir(inFile,outFile);
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
      "         \\,,,/\n" +
      "         (o o)\n" +
      "-----oOOo-(_)-oOOo-----")
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
      // custom catch Flink phrase:
      if (line == "writeFlinkVD") {
        writeFilesToDisk()
        return (true)
      }

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
