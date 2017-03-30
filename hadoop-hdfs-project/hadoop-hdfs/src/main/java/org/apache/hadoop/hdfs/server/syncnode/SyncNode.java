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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.EditLogTailerSyncNode;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

@InterfaceAudience.Private
public class SyncNode {

    static{
        HdfsConfiguration.init();
    }

    volatile private boolean running = true;

    private EditLogTailerSyncNode editLogTailer = null;

    public static final Logger LOG =
            LoggerFactory.getLogger(SyncNode.class.getName());

    private static final String USAGE = "Usage: hdfs syncnode";

    private JvmPauseMonitor pauseMonitor;

    public SyncNode(Configuration conf) throws IOException {
        try {
            initialize(conf);
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

        pauseMonitor = new JvmPauseMonitor(conf);
        pauseMonitor.start();

        startCommonServices(conf);
    }

    /**
     * Start the services common to active and standby states
     */
    private void startCommonServices(Configuration conf) throws IOException {
        editLogTailer = new EditLogTailerSyncNode(conf);
        editLogTailer.start();
//        namesystem.startCommonServices(conf, haContext);
//        registerNNSMXBean();
//        if (HdfsServerConstants.NamenodeRole.NAMENODE != role) {
//            startHttpServer(conf);
//            httpServer.setNameNodeAddress(getNameNodeAddress());
//            httpServer.setFSImage(getFSImage());
//        }
//        rpcServer.start();
//        plugins = conf.getInstances(DFS_NAMENODE_PLUGINS_KEY,
//                ServicePlugin.class);
//        for (ServicePlugin p: plugins) {
//            try {
//                p.start(this);
//            } catch (Throwable t) {
//                LOG.warn("ServicePlugin " + p + " could not be started", t);
//            }
//        }
//        LOG.info(getRole() + " RPC up at: " + rpcServer.getRpcAddress());
//        if (rpcServer.getServiceRpcAddress() != null) {
//            LOG.info(getRole() + " service RPC up at: "
//                    + rpcServer.getServiceRpcAddress());
//        }
    }

    private void stopCommonServices() {
//        if(rpcServer != null) rpcServer.stop();
//        if(namesystem != null) namesystem.close();
//        if (pauseMonitor != null) pauseMonitor.stop();
//        if (plugins != null) {
//            for (ServicePlugin p : plugins) {
//                try {
//                    p.stop();
//                } catch (Throwable t) {
//                    LOG.warn("ServicePlugin " + p + " could not be stopped", t);
//                }
//            }
//        }
//        stopHttpServer();
    }

    /**
     * Wait for service to finish.
     * (Normally, it runs forever.)
     */
    public void join() throws InterruptedException {
        while (running) {
            wait();
        }
//        try {
//            rpcServer.join();
//        } catch (InterruptedException ie) {
//            LOG.info("Caught interrupted exception ", ie);
//        }
    }

    /**
     * Stop all NameNode threads and wait for all to finish.
     */
    public void stop() {
        running = false;
//        synchronized(this) {
//            if (stopRequested)
//                return;
//            stopRequested = true;
//        }
//        try {
//            if (state != null) {
//                state.exitState(haContext);
//            }
//        } catch (ServiceFailedException e) {
//            LOG.warn("Encountered exception while exiting state ", e);
//        } finally {
//            stopCommonServices();
//            if (metrics != null) {
//                metrics.shutdown();
//            }
//            if (namesystem != null) {
//                namesystem.shutdown();
//            }
//            if (nameNodeStatusBeanName != null) {
//                MBeans.unregister(nameNodeStatusBeanName);
//                nameNodeStatusBeanName = null;
//            }
//        }
//        tracer.close();
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
