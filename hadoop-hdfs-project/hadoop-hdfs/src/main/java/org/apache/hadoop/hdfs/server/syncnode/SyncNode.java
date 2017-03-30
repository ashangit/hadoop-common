/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.syncnode;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.BackupNode;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.BootstrapStandby;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressMetrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.tracing.TracerConfigurationManager;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

@InterfaceAudience.Private
public class SyncNode {

    public static final Logger LOG =
            LoggerFactory.getLogger(NameNode.class.getName());

    private static final String USAGE = "Usage: hdfs syncnode";

    public SyncNode(Configuration conf) throws IOException {
        try {
            initializeGenericKeys(conf, nsId, namenodeId);
            initialize(conf);
            try {
                haContext.writeLock();
                state.prepareToEnterState(haContext);
                state.enterState(haContext);
            } finally {
                haContext.writeUnlock();
            }
        } catch (IOException e) {
            this.stop();
            throw e;
        } catch (HadoopIllegalArgumentException e) {
            this.stop();
            throw e;
        }
    }

    public static SyncNode createSyncNode(String argv[], Configuration conf)
            throws IOException {
        LOG.info("createSyncNode " + Arrays.asList(argv));
        if (conf == null)
            conf = new HdfsConfiguration();

        DefaultMetricsSystem.initialize("NameNode");
        return new SyncNode(conf);
    }

    protected void initialize(Configuration conf) throws IOException {
        if (conf.get(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS) == null) {
            String intervals = conf.get(DFS_METRICS_PERCENTILES_INTERVALS_KEY);
            if (intervals != null) {
                conf.set(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS,
                        intervals);
            }
        }

        UserGroupInformation.setConfiguration(conf);

        private JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(conf);
        pauseMonitor.start();
        metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);

        startCommonServices(conf);
    }

    /**
     */
    public static void main(String argv[]) throws Exception {
        if (DFSUtil.parseHelpArgument(argv, SyncNode.USAGE, System.out, true)) {
            System.exit(0);
        }

        try {
            StringUtils.startupShutdownMessage(SyncNode.class, argv, LOG);
            SyncNode syncnode = createSyncNode(argv, null);
            if (syncnode != null) {
                syncnode.join();
            }
        } catch (Throwable e) {
            LOG.error("Failed to start syncnode.", e);
            terminate(1, e);
        }
    }
}
