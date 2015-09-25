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

package org.apache.flink.client;

import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.*;

public class CliFrontendInfoTest extends TestLogger {

	private static PrintStream stdOut;
	private static PrintStream capture;
	private static ByteArrayOutputStream buffer;

	@Test
	public void testErrorCases() throws Exception {
		// test unrecognized option
		{
			String[] parameters = {"-v", "-l"};
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.cancel(parameters);
			assertTrue(retCode != 0);
		}

		// test missing options
		{
			String[] parameters = {};
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.cancel(parameters);
			assertTrue(retCode != 0);
		}
	}

	@Test
	public void testShowExecutionPlan() throws Exception {
		replaceStdOut();
		try {
			String[] parameters = new String[] { CliFrontendTestUtils.getTestJarPath() };
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.info(parameters);
			assertTrue(retCode == 0);
			assertTrue(buffer.toString().contains("\"parallelism\": \"1\""));
		} finally {
			restoreStdOut();
		}
	}

	@Test
	public void testShowExecutionPlanWithParallelism() throws Exception {
		replaceStdOut();
		try {
			String[] parameters = {"-p", "17", CliFrontendTestUtils.getTestJarPath()};
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.info(parameters);
			assertTrue(retCode == 0);
			assertTrue(buffer.toString().contains("\"parallelism\": \"17\""));
		} finally {
			restoreStdOut();
		}
	}

	private static void replaceStdOut() {
		stdOut = System.out;
		buffer = new ByteArrayOutputStream();
		capture = new PrintStream(buffer);
		System.setOut(capture);
	}

	private static void restoreStdOut() {
		System.setOut(stdOut);
	}
}
