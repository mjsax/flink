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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * {@link StoppableStreamSource} takes a {@link SourceFunction} that implements {@link StoppableFunction}.
 */
public class StoppableStreamSource<T> extends StreamSource<T> {

	private static final long serialVersionUID = -4365670858793587337L;

	/**
	 * Takes a {@link SourceFunction} that implements {@link StoppableFunction}.
	 * 
	 * @param sourceFunction
	 *            A {@link SourceFunction} that implements {@link StoppableFunction}.
	 * 
	 * @throw IllegalArgumentException if {@code sourceFunction} does not implement {@link StoppableFunction}
	 */
	public StoppableStreamSource(SourceFunction<T> sourceFunction) {
		super(sourceFunction);

		if (!(sourceFunction instanceof StoppableFunction)) {
			throw new IllegalArgumentException(
					"The given SourceFunction must implement StoppableFunction.");
		}
	}

	public void stop() {
		((StoppableFunction) userFunction).stop();
	}

}
