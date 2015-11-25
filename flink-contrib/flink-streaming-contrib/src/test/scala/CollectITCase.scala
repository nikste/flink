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

import java.util.concurrent.TimeUnit

import org.apache.flink.contrib.streaming.scala.DataStreamUtils._
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.{TestBaseUtils, ForkableFlinkMiniCluster}
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration.FiniteDuration

@RunWith(classOf[JUnitRunner])
class CollectITCase extends FunSuite with Matchers with BeforeAndAfterAll {

  var cluster: Option[ForkableFlinkMiniCluster] = None
  val parallelism = 4

  test("Streaming collect scala") {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createRemoteEnvironment(
      "localhost", cluster.getOrElse(null).getLeaderRPCPort)

    var N = 10;
    var stream = env.generateSequence(1, N);

    var i = 1;
    var it = collect(stream)
    while (it.hasNext())
    {
      var x = it.next;
      if (x != i) {
        Assert.fail("Should have got " + i + ", got " + x + " instead.")
      }
      i = i + 1;
    }
    if (i != N + 1) {
      Assert.fail("Should have collected " + N + " numbers, got " + (i - 1) + "instead.")
    }
  }

  override def beforeAll(): Unit = {
    val cl = TestBaseUtils.startCluster(
      1,
      parallelism,
      StreamingMode.BATCH_ONLY,
      false,
      false,
      false)

    cluster = Some(cl)
  }

  override def afterAll(): Unit = {
    cluster.foreach(c => TestBaseUtils.stopCluster(c, new FiniteDuration(1000, TimeUnit.SECONDS)))
  }
}
