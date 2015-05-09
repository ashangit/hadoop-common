/**
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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestLeaseManager {

  public static long maxLockHoldToReleaseLeaseMs = 100;
  
  @Test
  public void testRemoveLeases() throws Exception {
    FSNamesystem fsn = mock(FSNamesystem.class);
    LeaseManager lm = new LeaseManager(fsn);
    ArrayList<Long> ids = Lists.newArrayList(INodeId.ROOT_INODE_ID + 1,
            INodeId.ROOT_INODE_ID + 2, INodeId.ROOT_INODE_ID + 3,
            INodeId.ROOT_INODE_ID + 4);
    for (long id : ids) {
      lm.addLease("foo", id);
    }

    assertEquals(4, lm.getINodeIdWithLeases().size());
    synchronized (lm) {
      lm.removeLeases(ids);
    }
    assertEquals(0, lm.getINodeIdWithLeases().size());
  }

  /** Check that LeaseManager.checkLease release some leases
   */
  @Test (timeout=30000)
  public void testCheckLease() throws InterruptedException {
    LeaseManager lm = new LeaseManager(makeMockFsNameSystem());
    final long expiryTime = 0;
    final int leaseExpiryTime = 0;

    long numLease = 100;

    //Make sure the leases we are going to add exceed the hard limit
    lm.setLeasePeriod(leaseExpiryTime, leaseExpiryTime);

    for (long i = 0; i <= numLease - 1; i++) {
      //Add some leases to the LeaseManager
      lm.addLease("holder"+i, INodeId.ROOT_INODE_ID + i);
    }
    assertEquals(numLease, lm.countLease());

    //Initiate a call to checkLease. This should exit within the test timeout
    lm.checkLeases();
    assertTrue(lm.countLease() < numLease);
  }

  private static FSNamesystem makeMockFsNameSystem() {
    FSDirectory dir = mock(FSDirectory.class);
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(fsn.isRunning()).thenReturn(true);
    when(fsn.hasReadLock()).thenReturn(true);
    when(fsn.hasWriteLock()).thenReturn(true);
    when(fsn.getFSDirectory()).thenReturn(dir);
    when(fsn.getMaxLockHoldToReleaseLeaseMs()).thenReturn(maxLockHoldToReleaseLeaseMs);
    return fsn;
  }

  /**
   * Make sure the lease is restored even if only the inode has the record.
   */
  @Test
  public void testLeaseRestorationOnRestart() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
          .numDataNodes(1).build();
      DistributedFileSystem dfs = cluster.getFileSystem();

      // Create an empty file
      String path = "/testLeaseRestorationOnRestart";
      FSDataOutputStream out = dfs.create(new Path(path));

      // Remove the lease from the lease manager, but leave it in the inode.
      FSDirectory dir = cluster.getNamesystem().getFSDirectory();
      INodeFile file = dir.getINode(path).asFile();
      cluster.getNamesystem().leaseManager.removeLease(
          file.getFileUnderConstructionFeature().getClientName(), file);

      // Save a fsimage.
      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace();
      dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      // Restart the namenode.
      cluster.restartNameNode(true);

      // Check whether the lease manager has the lease
      assertNotNull("Lease should exist",
          cluster.getNamesystem().leaseManager.getLease(file) != null);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
