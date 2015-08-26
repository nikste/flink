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

package org.apache.flink.contrib.streaming.scala

import java.io.{DataInputStream, EOFException, IOException, InputStream}
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.DataInputView

/**
 * Created by nikste on 8/25/15.
 */

class StreamReaderDataInputView(
      stream : InputStream)
  extends DataInputStream(stream)
  with DataInputView {

  @throws[IOException]
  override def skipBytesToRead(numBytes: Int): Unit = {
    var tmpNumBytes = numBytes
    while ( tmpNumBytes > 0 ){
      var skipped : Int = skipBytes(tmpNumBytes)
      tmpNumBytes -= skipped
    }
  }
}

class DataStreamIterator[T](
      serializer : TypeSerializer[T],
      var nextElement : T = null.asInstanceOf[T],
      @volatile var streamReader : StreamReaderDataInputView = null,
      var socket: ServerSocket = null,
      var tcpStream: InputStream = null)
  extends Iterator[T]{

  val connectionAccepted : CountDownLatch = new CountDownLatch(1)

  try {
    var socket = new ServerSocket(0, 1, null)
  }
  catch {
    case e: IOException => {
      throw new
          RuntimeException(
            "DataStreamIterator: an I/O error occurred when opening the socket",
            e)
    }
      (new AcceptThread()).start()
  }

  class AcceptThread extends Thread {
    override def run() {
      try {
        val tcpStream = socket
          .accept()
          .getInputStream();
        streamReader =
          new StreamReaderDataInputView(tcpStream)

        connectionAccepted.countDown();
      } catch {
        case e: IOException =>
          throw new
              RuntimeException("DataStreamIterator.AcceptThread failed",
                e);
      }
    }
  }



  /**
   * Returns the port on which the iterator is getting the data. (Used internally.)
   * @return The port
   */
  def getPort() : Int = {
    socket.getLocalPort
  }

  /**
   * Returns true if the DataStream has more elements.
   * (Note: blocks if there will be more elements, but they are not available yet.)
   * @return true if the DataStream has more elements
   */
  override def hasNext: Boolean = {
    if (nextElement == null) {
      readNextFromStream();
    }
    return nextElement != null;
  }
  /**
   * Returns the next element of the DataStream. (Blocks if it is not available yet.)
   * @return The element
   * @throws NoSuchElementException if the stream has already ended
   */
  override def next(): T = {
    if (next == null.asInstanceOf[T]) {
      readNextFromStream()
      if (next == null.asInstanceOf[T]) {
        throw new NoSuchElementException()
      }
    }
    var current : T = nextElement
    nextElement = null.asInstanceOf[T]
    return current
  }

  def readNextFromStream() : Unit = {
    try {
      connectionAccepted.await();
    } catch {
      case e:InterruptedException =>
      throw new RuntimeException(
        "The calling thread of DataStreamIterator.readNextFromStream was interrupted.");
    }
    try {
      nextElement = serializer.deserialize(streamReader);
    } catch  {
      case e : EOFException =>
      nextElement = null.asInstanceOf[T]
      case e: IOException =>
      throw new RuntimeException(
        "DataStreamIterator could not read from deserializedStream",
        e);
    }
  }

}
