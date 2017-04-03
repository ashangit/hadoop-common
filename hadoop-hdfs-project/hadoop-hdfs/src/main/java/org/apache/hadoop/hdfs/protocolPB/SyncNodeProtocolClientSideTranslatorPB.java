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

package org.apache.hadoop.hdfs.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.SyncNodeProtocol;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This class is the client side translator to translate the requests made on
 * {@link SyncNodeProtocol} interfaces to the RPC server implementing
 * {@link SyncNodeProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class SyncNodeProtocolClientSideTranslatorPB implements
        ProtocolMetaInterface, SyncNodeProtocol, Closeable {

    /**
     * RpcController is not used and hence is set to null
     */
    private final SyncNodeProtocolPB rpcProxy;
    private static final VersionRequestProto VOID_VERSION_REQUEST =
            VersionRequestProto.newBuilder().build();
    private final static RpcController NULL_CONTROLLER = null;

    public SyncNodeProtocolClientSideTranslatorPB(InetSocketAddress nameNodeAddr,
                                                  Configuration conf) throws IOException {
        RPC.setProtocolEngine(conf, SyncNodeProtocolPB.class,
                ProtobufRpcEngine.class);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        rpcProxy = createNamenode(nameNodeAddr, conf, ugi);
    }

    private static SyncNodeProtocolPB createNamenode(
            InetSocketAddress nameNodeAddr, Configuration conf,
            UserGroupInformation ugi) throws IOException {
        return RPC.getProtocolProxy(SyncNodeProtocolPB.class,
                RPC.getProtocolVersion(SyncNodeProtocolPB.class), nameNodeAddr, ugi,
                conf, NetUtils.getSocketFactory(conf, SyncNodeProtocolPB.class),
                org.apache.hadoop.ipc.Client.getPingInterval(conf), null).getProxy();
    }

    @Override
    public void close() throws IOException {
        RPC.stopProxy(rpcProxy);
    }

    @Override
    public NamespaceInfo versionRequest() throws IOException {
        try {
            return PBHelper.convert(rpcProxy.versionRequest(NULL_CONTROLLER,
                    VOID_VERSION_REQUEST).getInfo());
        } catch (ServiceException e) {
            throw ProtobufHelper.getRemoteException(e);
        }
    }

    @Override
    public boolean isMethodSupported(String methodName) throws IOException {
        return false;
    }
}
