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

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import java.io.File;
import java.io.IOException;

/**
 * A {@link Storage} implementation for the {@link SyncNode}.
 * <p>
 * There is only a single directory per SN in the current design.
 */
class SNStorage extends Storage {

    SNStorage() {
        super(NodeType.SYNC_NODE);
    }

    protected StorageDirectory loadStorageDirectory(NamespaceInfo nsInfo, File dataDir, StartupOption startOpt)
            throws IOException {
        StorageDirectory sd = new StorageDirectory(dataDir, null, false);
        try {
            StorageState curState = sd.analyzeStorage(startOpt, this);
            // sd is locked but not opened
            switch (curState) {
                case NORMAL:
                    break;
                case NON_EXISTENT:
                    LOG.info("Storage directory " + dataDir + " does not exist");
                    throw new IOException("Storage directory " + dataDir
                            + " does not exist");
                case NOT_FORMATTED: // format
                    LOG.info("Storage directory " + dataDir + " is not formatted for "
                            + nsInfo.getBlockPoolID());
                    LOG.info("Formatting ...");
                    format(sd, nsInfo);
                    break;
                default:  // recovery part is common
                    sd.doRecover(curState);
            }

            setServiceLayoutVersion(getServiceLayoutVersion());
            writeProperties(sd);

            return sd;
        } catch (IOException ioe) {
            sd.unlock();
            throw ioe;
        }
    }

    void format(StorageDirectory sd, NamespaceInfo nsInfo) throws IOException {
        sd.clearDirectory(); // create directory
        this.layoutVersion = HdfsConstants.DATANODE_LAYOUT_VERSION;
        this.clusterID = nsInfo.getClusterID();
        this.namespaceID = nsInfo.getNamespaceID();
        this.cTime = 0;

        writeProperties(sd);
    }

    @Override
    public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
        return false;
    }
}
