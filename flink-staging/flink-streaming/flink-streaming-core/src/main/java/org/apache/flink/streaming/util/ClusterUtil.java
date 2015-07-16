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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.messages.JobManagerMessages.StopJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;



public class ClusterUtil {
	private static final Logger LOG = LoggerFactory.getLogger(ClusterUtil.class);

	private static LocalFlinkMiniCluster exec = null;

	/**
	 * Executes the given JobGraph locally, on a FlinkMiniCluster
	 * 
	 * @param jobGraph
	 *            jobGraph
	 * @param parallelism
	 *            numberOfTaskTrackers
	 * @param memorySize
	 *            memorySize
	 * @param printDuringExecution
	 *            printDuringExecution
	 * @param detached
	 *            run in background an return immediately or block until job is finishes
	 * @param customConf
	 *            Custom configuration for the LocalExecutor. Can be null.
	 * @return if {@code detached == true} a {@link JobSubmissionResult}; otherwise a {@link JobExecutionResult}
	 */
	public static JobSubmissionResult runOnMiniCluster(JobGraph jobGraph, int parallelism, long memorySize,
			boolean printDuringExecution, boolean detached, Configuration customConf)
					throws Exception {

		Configuration configuration = jobGraph.getJobConfiguration();

		configuration.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize);
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, parallelism);
		if(customConf != null) {
			configuration.addAll(customConf);
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Running on mini cluster");
		}

		try {
			exec = new LocalFlinkMiniCluster(configuration, true);
			exec.start();
			
			if (detached) {
				return exec.submitJobDetached(jobGraph);
			} else {
				return exec.submitJobAndWait(jobGraph, printDuringExecution);
			}
		} finally {
			if (exec != null && !detached) {
				exec.stop();
			}
		}
	}

	/**
	 * Start a job in a detached mode on a local mini cluster.
	 */
	public static JobSubmissionResult startOnMiniCluster(JobGraph jobGraph, int parallelism) throws Exception {
		return runOnMiniCluster(jobGraph, parallelism, -1, true, true, null);
	}

	public static void stopJobOnMiniCluser(JobID jobId) {
		final Configuration configuration = GlobalConfiguration.getConfiguration();
		FiniteDuration timeout = AkkaUtils.getTimeout(configuration);

		ActorGateway jobmanager = exec.getLeaderGateway(timeout);

		final Future<Object> response = jobmanager.ask(new StopJob(jobId), timeout);
		try {
			Await.result(response, timeout);
		} catch (final Exception e) {
			throw new RuntimeException("Stopping job with ID " + jobId + " failed", e);
		}
	}
}
