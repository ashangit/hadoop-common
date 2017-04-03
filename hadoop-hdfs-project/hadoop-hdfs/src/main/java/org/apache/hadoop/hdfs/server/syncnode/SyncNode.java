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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocolPB.SyncNodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.EditLogTailerSyncNode;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

@InterfaceAudience.Private
public class SyncNode {

    static {
        HdfsConfiguration.init();
    }

    volatile private boolean running = true;

    private EditLogTailerSyncNode editLogTailer = null;

    public static final Logger LOG =
            LoggerFactory.getLogger(SyncNode.class.getName());

    private static final String USAGE = "Usage: hdfs syncnode";

    private JvmPauseMonitor pauseMonitor;

    private SNStorage storage = null;

    private StorageDirectory sd = null;

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

        DefaultMetricsSystem.initialize("SyncNode");
        return new SyncNode(conf);
    }

    protected void initialize(Configuration conf) throws IOException {
        File syncnodeFolder = new File(conf.getTrimmed(DFSConfigKeys.DFS_SYNCNODE_DIR_KEY,
                DFSConfigKeys.DFS_SYNCNODE_DIR_DEFAULT));

        validateAndCreateSyncDir(syncnodeFolder);

        storage = new SNStorage();

        Map<String, Map<String, InetSocketAddress>> newAddressMap = DFSUtil
                .getNNServiceRpcAddressesForCluster(conf);

        NamespaceInfo nsInfo = null;
        for (Map<String, InetSocketAddress> valueMap : newAddressMap.values()) {
            for (InetSocketAddress value : valueMap.values()) {
                try {
                    LOG.info(value.getHostName());
                    SyncNodeProtocolClientSideTranslatorPB syncPB = new SyncNodeProtocolClientSideTranslatorPB(value, conf);
                    nsInfo = syncPB.versionRequest();
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                } finally {
                    break;
                }
            }
        }

        LOG.info(nsInfo.getClusterID());

        sd = storage.loadStorageDirectory(nsInfo, syncnodeFolder, HdfsServerConstants.StartupOption.REGULAR);

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
        editLogTailer = new EditLogTailerSyncNode(conf, sd);
        editLogTailer.start();
    }

    private void stopCommonServices() throws IOException {
        editLogTailer.stop();
    }

    private static void validateAndCreateSyncDir(File dir) throws IOException {
        if (!dir.isAbsolute()) {
            throw new IllegalArgumentException(
                    "Sync dir '" + dir + "' should be an absolute path");
        }

        DiskChecker.checkDir(dir);
    }

    /**
     * Wait for service to finish.
     * (Normally, it runs forever.)
     */
    public void join() throws InterruptedException {
        while (running) {
            synchronized (this) {
                wait(2000);
            }
        }
    }

    /**
     * Stop all NameNode threads and wait for all to finish.
     */
    public void stop() throws IOException {
        running = false;
        stopCommonServices();
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
