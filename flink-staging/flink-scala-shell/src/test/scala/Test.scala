

import java.io._
import java.net.URLClassLoader

import scala.collection.mutable.ArrayBuffer

import org.apache.flink.api.scala.FlinkILoop
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster

/**
 * Created by owner on 29-4-15.
 */
object Test {
  def main(args: Array[String] ): Unit ={

    println("entering main")
    val input : String =
      """
        val text = env.fromElements("To be, or not to be,--that is the question:--","Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune","Or to take arms against a sea of troubles,")
        val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.groupBy(0).sum(1)
        val a = counts.collect()
      """.stripMargin

    val output : String = processInShell(input)

    println("Printing result from shell:")
    println(output)
    print("exiting main")
  }

    /**
     * initializes new local cluster and processes commands given in input
     * @param input commands to be processed in the shell
     * @return output of shell
     */
    def processInShell(input : String): String ={

      val in = new BufferedReader(new StringReader(input + "\n"))
      val out = new StringWriter()
      val baos = new ByteArrayOutputStream()

      System.setOut(new PrintStream(baos))

      // new local cluster
      val cluster = new LocalFlinkMiniCluster(new Configuration, false)
      val host = "localhost"
      val port = cluster.getJobManagerRPCPort

      val cl = getClass.getClassLoader
      var paths = new ArrayBuffer[String]
      if (cl.isInstanceOf[URLClassLoader]) {
        val urlLoader = cl.asInstanceOf[URLClassLoader]
        for (url <- urlLoader.getURLs) {
          if (url.getProtocol == "file") {
            paths += url.getFile
          }
        }
      }
      val classpath = paths.mkString(File.pathSeparator)

      //val repl = new FlinkILoop(host, port, in,new PrintWriter(out))
      val repl = new FlinkILoop(host, port, in, new PrintWriter(out)) //new MyILoop();

      //repl.in = InteractiveReader()
      //repl.command(input)
      //repl.interpret(input)
      /*
      repl.settings = new Settings()
      repl.settings.usejavacp.value = true
      repl.createInterpreter()
      repl.initialize()
      */
      repl.process(Array("-classpath",classpath))

      //repl.process(Array("-classpath",classpath))

      //println("in.read:"+in.read())

      // closing interpreter
      repl.closeInterpreter
      //repl.interpretStartingWith(input);
      out.toString + baos.toString()
      /*
      repl.settings = new Settings()

      // enable this line to use scala in intellij
      repl.settings.usejavacp.value = true

      repl.process(repl.settings)

      print( out.toString)
      //repl.process(Array("-classpath", classpath))
      //repl.process(repl.settings);
      //val inputArr = Array(input)
      //repl.process(inputArr);
      // return result of input
      return "penis" //out.toString
      */
    }
}
