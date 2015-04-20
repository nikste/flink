package org.apache.flink.api.scala

import java.io.{File, FileOutputStream}

import scala.tools.nsc.interpreter.ILoop

/**
 * Created by Nikolaas Steenbergen on 16-4-15.
 */
class FlinkILoop extends ILoop {


  // flink execution environment
  val flinkEnv = ExecutionEnvironment.getExecutionEnvironment


  /**
   * writes contents of the compiled lines that have been executed in the shell into a "physical directory":
   * /tmp/scala_shell/
   */
  def writeFilesToDisk(): Unit = {
    val vd = intp.virtualDirectory

    var vdIt = vd.iterator

    var basePath = "/tmp/scala_shell/"
    var z = 0
    for (fi <- vdIt) {
      if (fi.isDirectory) {
        var fiIt = fi.iterator
        for (f <- fiIt) {
          println(f.path + f.name + ": isDir:" + f.isDirectory)
          // create directories
          var fullPath = basePath + z + "/"
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
        z += 1
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
      //intp.addImports("org.apache.flink.api.scala._")

      // with this we can access this object in the scala shell
      intp.bindValue("intp", this)
      intp.bindValue("env", this.flinkEnv)
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
