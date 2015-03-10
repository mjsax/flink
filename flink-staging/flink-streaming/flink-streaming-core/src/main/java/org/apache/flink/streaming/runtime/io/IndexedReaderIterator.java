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

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.streamrecord.OrderedStreamRecord;

public class IndexedReaderIterator<T> extends ReaderIterator<T> {

	protected final IndexedMutableReader<DeserializationDelegate<T>> reader;
	private final MergeBuffer channelMerger;
	
	public IndexedReaderIterator(
			IndexedMutableReader<DeserializationDelegate<T>> reader,
			TypeSerializer<T> serializer) {

		this(reader, serializer, false);
	}
	
	public IndexedReaderIterator(
			IndexedMutableReader<DeserializationDelegate<T>> reader,
			TypeSerializer<T> serializer, boolean ordered) {

		super(reader, serializer);
		
		this.reader = reader;
		if(ordered == true) {
			this.channelMerger = new MergeBuffer(this.reader.getNumberOfInputChannels());
		} else {
			this.channelMerger = null;
		}
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public T next(T target) throws IOException {
		// non ordering case
		if(this.channelMerger == null) {
			return super.next(target);
		}

		// ordering case
		OrderedStreamRecord<?> nextRecord = this.channelMerger.getNext();
		if(nextRecord == null) {
			nextRecord = (OrderedStreamRecord<?>)super.next(target);
			if(nextRecord != null) {
				this.channelMerger.addTuple(nextRecord, this.reader.getLastChannelIndex());
				nextRecord = this.channelMerger.getNext();
			}
		}

		return (T)nextRecord;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T next() throws IOException {
		// non ordering case
		if(this.channelMerger == null) {
			return super.next();
		}

		// ordering case
		OrderedStreamRecord<?> nextRecord = this.channelMerger.getNext();
		if(nextRecord == null) {
			nextRecord = (OrderedStreamRecord<?>)super.next();
			if(nextRecord != null) {
				this.channelMerger.addTuple(nextRecord, this.reader.getLastChannelIndex());
				nextRecord = this.channelMerger.getNext();
			}
		}

		return (T)nextRecord;
	}
	
}
