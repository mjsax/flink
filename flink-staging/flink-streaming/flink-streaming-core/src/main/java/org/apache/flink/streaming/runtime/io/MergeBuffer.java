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

package org.apache.flink.streaming.runtime.io;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * TODO
 * 
 *
 * @param <IN>
 */
class MergeBuffer<T> {
	private static final Logger LOG = LoggerFactory.getLogger(MergeBuffer.class);

	/**
	 * TODO
	 */
	private final LinkedList<T>[] channelBuffer;
	/**
	 * TODO
	 */
	private long latestTs = Long.MIN_VALUE;
	/**
	 * TODO
	 */
	private final Timestamp<T> timestampExtractor;
	
	
	
	/**
	 * TODO
	 * 
	 * @param numberOfInputChannels
	 * @param timestampExtractor
	 */
	@SuppressWarnings("unchecked")
	public MergeBuffer(final int numberOfInputChannels, Timestamp<T> timestampExtractor) {
		assert(numberOfInputChannels >= 0);
		
		this.channelBuffer = new LinkedList[numberOfInputChannels];
		for(int i = 0; i < numberOfInputChannels; ++i) {
			this.channelBuffer[i] = new LinkedList<T>();
		}

		this.timestampExtractor = timestampExtractor;
	}
	
	
	
	/**
	 * TODO
	 */
	public void addTuple(T record, int index) {
		assert(record != null);
		this.channelBuffer[index].add(record);
	}
	
	/**
	 * TODO
	 */
	public T getNext() {
		long minTsFound = Long.MAX_VALUE;
		boolean eachBufferFilled = true;
		int minTsPartitionNumber = -1;
		
		for(int i = 0; i < this.channelBuffer.length; ++i) {
			try {
				long ts = this.timestampExtractor.getTimestamp(this.channelBuffer[i].getFirst());
				assert (ts >= this.latestTs);
				
				if(ts == this.latestTs) {
					LOG.trace("Extract tuple with same timestamp (input channel, tuple): {}, {}",
						new Integer(i), this.channelBuffer[i].getFirst());
					return this.channelBuffer[i].removeFirst();
				}
				
				if(ts < minTsFound) {
					minTsFound = ts;
					minTsPartitionNumber = i;
				}
			} catch(NoSuchElementException e) {
				// in this case, we stay in the loop, because we still might find a tuple with equal ts value as last
				// returned tuple
				LOG.trace("Found empty input channel: {}", new Integer(i));
				eachBufferFilled = false;
			}
		}
		
		if(eachBufferFilled && minTsPartitionNumber != -1) {
			LOG.trace("Extract tuple min timestamp (ts, input channel, tuple): {}, {}", minTsFound,
				minTsPartitionNumber, this.channelBuffer[minTsPartitionNumber].getFirst());
			this.latestTs = minTsFound;
			return this.channelBuffer[minTsPartitionNumber].removeFirst();
		}
		
		LOG.trace("Could not extract record.");
		return null;
	}
	
}
