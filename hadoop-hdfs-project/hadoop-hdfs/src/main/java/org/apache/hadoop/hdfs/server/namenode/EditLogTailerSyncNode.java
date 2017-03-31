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
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.security.SecurityUtil;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SYNCNODE_REMOTE_SHARED_EDITS_DIR_KEY;


/**
 * EditLogTailer represents a thread which periodically reads from edits
 * journals and applies the transactions contained within to a given
 * FSNamesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EditLogTailerSyncNode {
    public static final Log LOG = LogFactory.getLog(EditLogTailerSyncNode.class);

    private final EditLogTailerSyncNodeThread tailerThread;

    private final Configuration conf;
    private FSEditLog editLog;

    protected NNStorage storage;

    /**
     * Take this lock when adding journals to or closing the JournalSet. Allows
     * us to ensure that the JournalSet isn't closed or updated underneath us
     * in selectInputStreams().
     */
    private final Object journalSetLock = new Object();

    /**
     * How often the Standby should roll edit logs. Since the Standby only reads
     * from finalized log segments, the Standby will only be as up-to-date as how
     * often the logs are rolled.
     */
    private final long logRollPeriodMs;

    private final String syncnodeFolder;

    /**
     * How often the Standby should check if there are new finalized segment(s)
     * available to be read from.
     */
    private final long sleepTimeMs;
    private long lastTxnId;

    public EditLogTailerSyncNode(Configuration conf) throws IOException {
        this.tailerThread = new EditLogTailerSyncNodeThread();
        this.conf = conf;

        List<URI> sharedEditsDirs = getRemoteSharedEditsDirs(conf);

        LOG.info(sharedEditsDirs);

        storage = new NNStorage(conf, Lists.<URI>newArrayList(), sharedEditsDirs);

        this.editLog = new FSEditLog(conf, storage, sharedEditsDirs);
        this.editLog.initSharedJournalsForRead();

        logRollPeriodMs = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
                DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT) * 1000;

        sleepTimeMs = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
                DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT) * 1000;

        syncnodeFolder = conf.getTrimmed(DFSConfigKeys.DFS_SYNCNODE_EDITS_DIR_KEY,
                DFSConfigKeys.DFS_SYNCNODE_EDITS_DIR_DEFAULT);

        LOG.debug("logRollPeriodMs=" + logRollPeriodMs +
                " sleepTime=" + sleepTimeMs);
    }

    /**
     * Returns edit directories that are shared between primary and secondary.
     *
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
        String lastEditFile = syncnodeFolder + "/lastTxnId";
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        DataInputStream dis = null;

        long lastTxnId = 0;


        try {
            Path path = Paths.get(lastEditFile);
            if (path.toFile().isFile()) {
                List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

                if (lines.size() > 0) {
                    lastTxnId = Long.parseLong(lines.get(0));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lastTxnId;
    }

    long loadEdits(Iterable<EditLogInputStream> editStreams, FSEditLogLoaderSyncNode loader) throws IOException {
        long lastAppliedTxId = lastTxnId;
        try {
            for (EditLogInputStream editIn : editStreams) {
                try {
                    loader.loadFSEdits(editIn);
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

    public Collection<EditLogInputStream> selectInputStreams(
            long fromTxId, long toAtLeastTxId, MetaRecoveryContext recovery,
            boolean inProgressOk) throws IOException {

        List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
        synchronized (journalSetLock) {
            editLog.selectInputStreams(streams, fromTxId, inProgressOk);
        }

        try {
            editLog.checkForGaps(streams, fromTxId, toAtLeastTxId, inProgressOk);
        } catch (IOException e) {
            if (recovery != null) {
                // If recovery mode is enabled, continue loading even if we know we
                // can't load up to toAtLeastTxId.
                LOG.error(e);
            } else {
                editLog.closeAllStreams(streams);
                throw e;
            }
        }
        return streams;
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

    /**
     * The thread which does the actual work of tailing edits journals and
     * applying the transactions to the FSNS.
     */
    private class EditLogTailerSyncNodeThread extends Thread {
        private volatile boolean shouldRun = true;

        private EditLogTailerSyncNodeThread() {
            super("Edit log tailer syncnode");
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
                    doTailEdits();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
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
