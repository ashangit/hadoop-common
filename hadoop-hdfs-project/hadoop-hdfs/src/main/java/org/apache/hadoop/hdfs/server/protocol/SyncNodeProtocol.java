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

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/**********************************************************************
 * Protocol that a DFS syncnode uses to communicate with the NameNode.
 *
 **********************************************************************/
@KerberosInfo(
        serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
        clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface SyncNodeProtocol {
    /**
     * This class is used by both the Namenode (client) and BackupNode (server)
     * to insulate from the protocol serialization.
     *
     * If you are adding/changing DN's interface then you need to
     * change both this class and ALSO related protocol buffer
     * wire protocol definition in DatanodeProtocol.proto.
     *
     * For more details on protocol buffer wire protocol, please see
     * .../org/apache/hadoop/hdfs/protocolPB/overview.html
     */
    public static final long versionID = 1L;

    @Idempotent
    public NamespaceInfo versionRequest() throws IOException;
}
