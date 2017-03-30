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

package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SYNCNODE_REMOTE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.monotonicNow;


/**
 * EditLogTailer represents a thread which periodically reads from edits
 * journals and applies the transactions contained within to a given
 * FSNamesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EditLogTailerSyncNode {
  public static final Log LOG = LogFactory.getLog(EditLogTailerSyncNode.class);

  private final EditLogTailerThread tailerThread;

  private final Configuration conf;
  private FSEditLog editLog;

  private InetSocketAddress activeAddr;
  private NamenodeProtocol cachedActiveProxy = null;

  protected NNStorage storage;

  /**
   * The last transaction ID at which an edit log roll was initiated.
   */
  private long lastRollTriggerTxId = HdfsConstants.INVALID_TXID;

  /**
   * The highest transaction ID loaded by the Standby.
   */
  private long lastLoadedTxnId = HdfsConstants.INVALID_TXID;

  /**
   * The last time we successfully loaded a non-zero number of edits from the
   * shared directory.
   */
  private long lastLoadTimeMs;

  /**
   * How often the Standby should roll edit logs. Since the Standby only reads
   * from finalized log segments, the Standby will only be as up-to-date as how
   * often the logs are rolled.
   */
  private final long logRollPeriodMs;

  /**
   * How often the Standby should check if there are new finalized segment(s)
   * available to be read from.
   */
  private final long sleepTimeMs;
  private long lastTxnId;

  public EditLogTailerSyncNode(Configuration conf) throws IOException {
    this.tailerThread = new EditLogTailerThread();
    this.conf = conf;

    List<URI> sharedEditsDirs = getRemoteSharedEditsDirs(conf);

    storage = new NNStorage(conf, Lists.<URI>newArrayList(), sharedEditsDirs);

    this.editLog = new FSEditLog(conf, storage, sharedEditsDirs);

    // Get edit log from somewhere
    // this.editLog = namesystem.getEditLog();

    lastLoadTimeMs = monotonicNow();

    logRollPeriodMs = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
            DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT) * 1000;
    if (logRollPeriodMs >= 0) {
      this.activeAddr = getActiveNodeAddress();
      Preconditions.checkArgument(activeAddr.getPort() > 0,
              "Active NameNode must have an IPC port configured. " +
                      "Got address '%s'", activeAddr);
      LOG.info("Will roll logs on active node at " + activeAddr + " every " +
              (logRollPeriodMs / 1000) + " seconds.");
    } else {
      LOG.info("Not going to trigger log rolls on active node because " +
              DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY + " is negative.");
    }

    sleepTimeMs = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
            DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT) * 1000;

    LOG.debug("logRollPeriodMs=" + logRollPeriodMs +
            " sleepTime=" + sleepTimeMs);
  }

  private InetSocketAddress getActiveNodeAddress() {
    Configuration activeConf = HAUtil.getConfForOtherNode(conf);
    return NameNode.getServiceAddress(activeConf, true);
  }

  private NamenodeProtocol getActiveNodeProxy() throws IOException {
    if (cachedActiveProxy == null) {
      int rpcTimeout = conf.getInt(
              DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_KEY,
              DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_DEFAULT);
      NamenodeProtocolPB proxy = RPC.waitForProxy(NamenodeProtocolPB.class,
              RPC.getProtocolVersion(NamenodeProtocolPB.class), activeAddr, conf,
              rpcTimeout, Long.MAX_VALUE);
      cachedActiveProxy = new NamenodeProtocolTranslatorPB(proxy);
    }
    assert cachedActiveProxy != null;
    return cachedActiveProxy;
  }

  public void start() {
    tailerThread.start();
  }

  public void stop() throws IOException {
    tailerThread.setShouldRun(false);
    tailerThread.interrupt();
    try {
      tailerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    }
  }

  public void catchupDuringFailover() throws IOException {
    Preconditions.checkState(tailerThread == null ||
                    !tailerThread.isAlive(),
            "Tailer thread should not be running once failover starts");
    // Important to do tailing as the login user, in case the shared
    // edits storage is implemented by a JournalManager that depends
    // on security credentials to access the logs (eg QuorumJournalManager).
    SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          // It is already under the full name system lock and the checkpointer
          // thread is already stopped. No need to acqure any other lock.
          doTailEdits();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        return null;
      }
    });
  }

  /**
   * Returns edit directories that are shared between primary and secondary.
   * @param conf configuration
   * @return collection of edit directories from {@code conf}
   */
  public static List<URI> getRemoteSharedEditsDirs(Configuration conf) {
    // don't use getStorageDirs here, because we want an empty default
    // rather than the dir in /tmp
    Collection<String> dirNames = conf.getTrimmedStringCollection(
            DFS_SYNCNODE_REMOTE_SHARED_EDITS_DIR_KEY);
    return Util.stringCollectionAsURIs(dirNames);
  }

  long getLastTxFromFile() {
    File lastEditFile = new File("/tmp/syncnode/lastTxnId");
    FileInputStream fis = null;
    BufferedInputStream bis = null;
    DataInputStream dis = null;

    long lastTxnId = 0;

    try {
      fis = new FileInputStream(lastEditFile);

      bis = new BufferedInputStream(fis);
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);

      lastTxnId = Long.parseLong(br.readLine());

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        fis.close();
        bis.close();
        dis.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    return lastTxnId;
  }

  long loadEdits(Iterable<EditLogInputStream> editStreams, FSEditLogLoaderSyncNode loader) throws IOException {
    long lastAppliedTxId=lastTxnId;
    try{
      for (EditLogInputStream editIn : editStreams) {
        try {
          loader.loadFSEdits(editIn, lastTxnId + 1, null, null);
        } finally {
          // Update lastAppliedTxId even in case of error, since some ops may
          // have been successfully applied before the error.
          lastAppliedTxId = loader.getLastAppliedTxId();
        }
        // If we are in recovery mode, we may have skipped over some txids.
        if (editIn.getLastTxId() != HdfsConstants.INVALID_TXID) {
          lastAppliedTxId = editIn.getLastTxId();
        }
      }
    } finally {
      FSEditLog.closeAllStreams(editStreams);
      // update the counts
      //before: updateCountForQuota(target.dir.rootDir, quotaInitThreads);
    }
    return lastAppliedTxId - lastTxnId;
  }

  @VisibleForTesting
  void doTailEdits() throws IOException, InterruptedException {
    // Write lock needs to be interruptible here because the
    // transitionToActive RPC takes the write lock before calling
    // tailer.stop() -- so if we're not interruptible, it will
    // deadlock.
    lastTxnId = getLastTxFromFile();

    if (LOG.isDebugEnabled()) {
      LOG.debug("lastTxnId: " + lastTxnId);
    }
    Collection<EditLogInputStream> streams;
    try {
      streams = editLog.selectInputStreams(lastTxnId + 1, 0, null, false);
    } catch (IOException ioe) {
      // This is acceptable. If we try to tail edits in the middle of an edits
      // log roll, i.e. the last one has been finalized but the new inprogress
      // edits file hasn't been started yet.
      LOG.warn("Edits tailer failed to find any streams. Will try again " +
              "later.", ioe);
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("edit streams to load from: " + streams.size());
    }

    // Once we have streams to load, errors encountered are legitimate cause
    // for concern, so we don't catch them here. Simple errors reading from
    // disk are ignored.
    long editsLoaded = 0;
    try {
      //before: editsLoaded = image.loadEdits(streams, namesystem);
      FSEditLogLoaderSyncNode loader = new FSEditLogLoaderSyncNode(conf, lastTxnId);
      loadEdits(streams, loader);
    } finally {
      if (editsLoaded > 0 || LOG.isDebugEnabled()) {
        LOG.info(String.format("Loaded %d edits starting from txid %d ",
                editsLoaded, lastTxnId));
      }
    }

    if (editsLoaded > 0) {
      lastLoadTimeMs = monotonicNow();
    }
  }

  /**
   * @return true if the configured log roll period has elapsed.
   */
  private boolean tooLongSinceLastLoad() {
    return logRollPeriodMs >= 0 &&
            (monotonicNow() - lastLoadTimeMs) > logRollPeriodMs;
  }

  /**
   * Trigger the active node to roll its logs.
   */
  private void triggerActiveLogRoll() {
    LOG.info("Triggering log roll on remote NameNode " + activeAddr);
    try {
      getActiveNodeProxy().rollEditLog();
      lastRollTriggerTxId = lastLoadedTxnId;
    } catch (IOException ioe) {
      LOG.warn("Unable to trigger a roll of the active NN", ioe);
    }
  }

  /**
   * The thread which does the actual work of tailing edits journals and
   * applying the transactions to the FSNS.
   */
  private class EditLogTailerThread extends Thread {
    private volatile boolean shouldRun = true;

    private EditLogTailerThread() {
      super("Edit log tailer");
    }

    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }

    @Override
    public void run() {
      SecurityUtil.doAsLoginUserOrFatal(
              new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                  doWork();
                  return null;
                }
              });
    }

    private void doWork() {
      while (shouldRun) {
        try {
          // There's no point in triggering a log roll if the Standby hasn't
          // read any more transactions since the last time a roll was
          // triggered.
          if (tooLongSinceLastLoad() &&
                  lastRollTriggerTxId < lastLoadedTxnId) {
            triggerActiveLogRoll();
          }
          /**
           * Check again in case someone calls {@link EditLogTailerSyncNode#stop} while
           * we're triggering an edit log roll, since ipc.Client catches and
           * ignores {@link InterruptedException} in a few places. This fixes
           * the bug described in HDFS-2823.
           */
          if (!shouldRun) {
            break;
          }
        } catch (Throwable t) {
          LOG.fatal("Unknown error encountered while tailing edits. " +
                  "Shutting down standby NN.", t);
          terminate(1, t);
        }

        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOG.warn("Edit log tailer interrupted", e);
        }
      }
    }
  }

}
