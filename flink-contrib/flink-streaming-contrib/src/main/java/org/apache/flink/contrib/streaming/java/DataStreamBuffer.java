/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.java;



import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DataStreamBuffer<T> extends Thread {

	BlockingQueue<T> items = new LinkedBlockingQueue<T>();

	DataStreamIterator<T> dsi;

	private boolean running;

	public DataStreamBuffer(DataStream<T> stream) {
		dsi = DataStreamUtils.collect(stream);
		running = true;
		start();
	}

	@Override
	public void run() {
		super.run();

		// continually get items here
		while(running){
			if(dsi.hasNext()) {
				T nextElement = dsi.next();
				//TODO: how to handle full queues? is handled?
				boolean canAdd = items.offer(nextElement);
			}
		}
	}

	public void stopBuffer(){
		this.running = false;
	}

	public void pullElements(Collection<T> c){
		items.drainTo(c);
	}
}
