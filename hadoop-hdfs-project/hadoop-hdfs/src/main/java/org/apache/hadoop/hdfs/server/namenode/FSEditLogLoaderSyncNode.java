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

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCloseOp;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.util.Holder;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumMap;

import static org.apache.hadoop.hdfs.server.namenode.FSImageFormat.renameReservedPathsOnUpgrade;
import static org.apache.hadoop.util.Time.monotonicNow;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLogLoaderSyncNode {
    static final Log LOG = LogFactory.getLog(FSEditLogLoaderSyncNode.class.getName());
    static final long REPLAY_TRANSACTION_LOG_INTERVAL = 1000; // 1sec

    private long lastAppliedTxId;
    /**
     * Total number of end transactions loaded.
     */
    private int totalEdits = 0;

    private final int replication;

    public FSEditLogLoaderSyncNode(Configuration conf, long lastAppliedTxId) {
        this.lastAppliedTxId = lastAppliedTxId;
        replication = conf.getInt(
                DFSConfigKeys.DFS_SYNCNODE_REPLICATION_KEY,
                DFSConfigKeys.DFS_SYNCNODE_REPLICATION_DEFAULT);
    }

    /**
     * Load an edit log, and apply the changes to the in-memory structure
     * This is where we apply edits that we've been writing to disk all
     * along.
     */
    long loadFSEdits(EditLogInputStream edits) throws IOException {
        try {
            long startTime = monotonicNow();
            FSImage.LOG.info("Start loading edits file " + edits.getName());
            long numEdits = loadEditRecords(edits);
            FSImage.LOG.info("Edits file " + edits.getName()
                    + " of size " + edits.length() + " edits # " + numEdits
                    + " loaded in " + (monotonicNow() - startTime) / 1000 + " seconds");
            return numEdits;
        } finally {
            edits.close();
        }
    }

    long loadEditRecords(EditLogInputStream in) throws IOException {
        long numEdits = 0;
        try {
            while (true) {
                FSEditLogOp op;
                try {
                    op = in.readOp();
                    if (op == null) {
                        break;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    continue;
                }
                applyEditLogOp(op, in.getVersion(true));
                numEdits++;
            }
        } finally {
            if (LOG.isTraceEnabled()) {
                LOG.trace("replaying edit log finished");
            }
        }
        return numEdits;
    }

    private long applyEditLogOp(FSEditLogOp op, int logVersion) throws IOException {
        long inodeId = INodeId.GRANDFATHER_INODE_ID;
        if (LOG.isTraceEnabled()) {
            LOG.trace("replaying edit log: " + op);
        }

        switch (op.opCode) {
            case OP_ADD: {
                AddCloseOp addCloseOp = (AddCloseOp) op;
                final String path =
                        renameReservedPathsOnUpgrade(addCloseOp.path, logVersion);
                LOG.info("path: " + path);
                break;
                // There three cases here:
                // 1. OP_ADD to create a new file
                // 2. OP_ADD to update file blocks
                // 3. OP_ADD to open file for append

                // TODO: we must have a way to check if the file already exist (use a new rpc interface ????)
                // See if the file already exists (persistBlocks call)
//        final INodesInPath iip = fsDir.getINodesInPath(path, true);
//        final INode[] inodes = iip.getINodes();
//        INodeFile oldFile = INodeFile.valueOf(
//                inodes[inodes.length - 1], path, true);
//        if (oldFile != null && addCloseOp.overwrite) {
//          // This is OP_ADD with overwrite
//          fsDir.unprotectedDelete(path, addCloseOp.mtime);
//          oldFile = null;
//        }
//                INodeFile oldFile = null;
//                INodeFile newFile = oldFile;
//                if (oldFile == null) { // this is OP_ADD on a new file (case 1)
//                    // versions > 0 support per file replication
//                    // get name and replication
//                    assert addCloseOp.blocks.length == 0;
//
//                    // add to the file tree
//                    //inodeId = getAndUpdateLastInodeId(addCloseOp.inodeId, logVersion,
//                    //        lastInodeId);
//                    newFile = fsDir.unprotectedAddFile(inodeId,
//                            path, addCloseOp.permissions, addCloseOp.aclEntries,
//                            addCloseOp.xAttrs,
//                            replication, addCloseOp.mtime, addCloseOp.atime,
//                            addCloseOp.blockSize, true, addCloseOp.clientName,
//                            addCloseOp.clientMachine, addCloseOp.storagePolicyId);
//                    fsNamesys.leaseManager.addLease(addCloseOp.clientName, path);
//
//                    // add the op into retry cache if necessary
//                    if (toAddRetryCache) {
//                        HdfsFileStatus stat = fsNamesys.dir.createFileStatus(
//                                HdfsFileStatus.EMPTY_NAME, newFile,
//                                BlockStoragePolicySuite.ID_UNSPECIFIED, Snapshot.CURRENT_STATE_ID,
//                                false, iip);
//                        fsNamesys.addCacheEntryWithPayload(addCloseOp.rpcClientId,
//                                addCloseOp.rpcCallId, stat);
//                    }
//                }
                // TODO treat file already exist case
//        else { // This is OP_ADD on an existing file
//          if (!oldFile.isUnderConstruction()) {
//            // This is case 3: a call to append() on an already-closed file.
//            if (FSNamesystem.LOG.isDebugEnabled()) {
//              FSNamesystem.LOG.debug("Reopening an already-closed file " +
//                      "for append");
//            }
//            LocatedBlock lb = fsNamesys.prepareFileForWrite(path,
//                    oldFile, addCloseOp.clientName, addCloseOp.clientMachine, false, iip.getLatestSnapshotId(), false);
//            newFile = INodeFile.valueOf(fsDir.getINode(path),
//                    path, true);
//
//            // add the op into retry cache is necessary
//            if (toAddRetryCache) {
//              fsNamesys.addCacheEntryWithPayload(addCloseOp.rpcClientId,
//                      addCloseOp.rpcCallId, lb);
//            }
//          }
//        }
                // Fall-through for case 2.
                // Regardless of whether it's a new file or an updated file,
                // update the block list.

                // Update the salient file attributes.
//                newFile.setAccessTime(addCloseOp.atime, Snapshot.CURRENT_STATE_ID);
//                newFile.setModificationTime(addCloseOp.mtime, Snapshot.CURRENT_STATE_ID);
//                updateBlocks(fsDir, addCloseOp, newFile);
//                break;
            }
//            case OP_CLOSE: {
//                AddCloseOp addCloseOp = (AddCloseOp) op;
//                final String path =
//                        renameReservedPathsOnUpgrade(addCloseOp.path, logVersion);
//                if (FSNamesystem.LOG.isDebugEnabled()) {
//                    FSNamesystem.LOG.debug(op.opCode + ": " + path +
//                            " numblocks : " + addCloseOp.blocks.length +
//                            " clientHolder " + addCloseOp.clientName +
//                            " clientMachine " + addCloseOp.clientMachine);
//                }
//
//                final INodesInPath iip = fsDir.getLastINodeInPath(path);
//                final INodeFile file = INodeFile.valueOf(iip.getINode(0), path);
//
//                // Update the salient file attributes.
//                file.setAccessTime(addCloseOp.atime, Snapshot.CURRENT_STATE_ID);
//                file.setModificationTime(addCloseOp.mtime, Snapshot.CURRENT_STATE_ID);
//                updateBlocks(fsDir, addCloseOp, file);
//
//                // Now close the file
//                if (!file.isUnderConstruction() &&
//                        logVersion <= LayoutVersion.BUGFIX_HDFS_2991_VERSION) {
//                    // There was a bug (HDFS-2991) in hadoop < 0.23.1 where OP_CLOSE
//                    // could show up twice in a row. But after that version, this
//                    // should be fixed, so we should treat it as an error.
//                    throw new IOException(
//                            "File is not under construction: " + path);
//                }
//                // One might expect that you could use removeLease(holder, path) here,
//                // but OP_CLOSE doesn't serialize the holder. So, remove by path.
//                if (file.isUnderConstruction()) {
//                    fsNamesys.leaseManager.removeLeaseWithPrefixPath(path);
//                    file.toCompleteFile(file.getModificationTime());
//                }
//                break;
//            }
//            case OP_UPDATE_BLOCKS: {
//                UpdateBlocksOp updateOp = (UpdateBlocksOp) op;
//                final String path =
//                        renameReservedPathsOnUpgrade(updateOp.path, logVersion);
//                if (FSNamesystem.LOG.isDebugEnabled()) {
//                    FSNamesystem.LOG.debug(op.opCode + ": " + path +
//                            " numblocks : " + updateOp.blocks.length);
//                }
//                INodeFile oldFile = INodeFile.valueOf(fsDir.getINode(path),
//                        path);
//                // Update in-memory data structures
//                updateBlocks(fsDir, updateOp, oldFile);
//
//                if (toAddRetryCache) {
//                    fsNamesys.addCacheEntry(updateOp.rpcClientId, updateOp.rpcCallId);
//                }
//                break;
//            }
//            case OP_ADD_BLOCK: {
//                AddBlockOp addBlockOp = (AddBlockOp) op;
//                String path = renameReservedPathsOnUpgrade(addBlockOp.getPath(), logVersion);
//                if (FSNamesystem.LOG.isDebugEnabled()) {
//                    FSNamesystem.LOG.debug(op.opCode + ": " + path +
//                            " new block id : " + addBlockOp.getLastBlock().getBlockId());
//                }
//                INodeFile oldFile = INodeFile.valueOf(fsDir.getINode(path), path);
//                // add the new block to the INodeFile
//                addNewBlock(fsDir, addBlockOp, oldFile);
//                break;
//            }
//            case OP_SET_REPLICATION: {
//                SetReplicationOp setReplicationOp = (SetReplicationOp) op;
//                short replication = fsNamesys.getBlockManager().adjustReplication(
//                        setReplicationOp.replication);
//                fsDir.unprotectedSetReplication(
//                        renameReservedPathsOnUpgrade(setReplicationOp.path, logVersion),
//                        replication, null);
//                break;
//            }
//            case OP_CONCAT_DELETE: {
//                ConcatDeleteOp concatDeleteOp = (ConcatDeleteOp) op;
//                String trg = renameReservedPathsOnUpgrade(concatDeleteOp.trg, logVersion);
//                String[] srcs = new String[concatDeleteOp.srcs.length];
//                for (int i = 0; i < srcs.length; i++) {
//                    srcs[i] =
//                            renameReservedPathsOnUpgrade(concatDeleteOp.srcs[i], logVersion);
//                }
//                fsDir.unprotectedConcat(trg, srcs, concatDeleteOp.timestamp);
//
//                if (toAddRetryCache) {
//                    fsNamesys.addCacheEntry(concatDeleteOp.rpcClientId,
//                            concatDeleteOp.rpcCallId);
//                }
//                break;
//            }
//            case OP_RENAME_OLD: {
//                RenameOldOp renameOp = (RenameOldOp) op;
//                final String src = renameReservedPathsOnUpgrade(renameOp.src, logVersion);
//                final String dst = renameReservedPathsOnUpgrade(renameOp.dst, logVersion);
//                fsDir.unprotectedRenameTo(src, dst,
//                        renameOp.timestamp);
//
//                if (toAddRetryCache) {
//                    fsNamesys.addCacheEntry(renameOp.rpcClientId, renameOp.rpcCallId);
//                }
//                break;
//            }
//            case OP_DELETE: {
//                DeleteOp deleteOp = (DeleteOp) op;
//                fsDir.unprotectedDelete(
//                        renameReservedPathsOnUpgrade(deleteOp.path, logVersion),
//                        deleteOp.timestamp);
//
//                if (toAddRetryCache) {
//                    fsNamesys.addCacheEntry(deleteOp.rpcClientId, deleteOp.rpcCallId);
//                }
//                break;
//            }
//            case OP_MKDIR: {
//                MkdirOp mkdirOp = (MkdirOp) op;
//                inodeId = getAndUpdateLastInodeId(mkdirOp.inodeId, logVersion,
//                        lastInodeId);
//                fsDir.unprotectedMkdir(inodeId,
//                        renameReservedPathsOnUpgrade(mkdirOp.path, logVersion),
//                        mkdirOp.permissions, mkdirOp.aclEntries, mkdirOp.timestamp);
//                break;
//            }
//            case OP_SET_GENSTAMP_V1: {
//                SetGenstampV1Op setGenstampV1Op = (SetGenstampV1Op) op;
//                fsNamesys.setGenerationStampV1(setGenstampV1Op.genStampV1);
//                break;
//            }
//            case OP_SET_PERMISSIONS: {
//                SetPermissionsOp setPermissionsOp = (SetPermissionsOp) op;
//                fsDir.unprotectedSetPermission(
//                        renameReservedPathsOnUpgrade(setPermissionsOp.src, logVersion),
//                        setPermissionsOp.permissions);
//                break;
//            }
//            case OP_SET_OWNER: {
//                SetOwnerOp setOwnerOp = (SetOwnerOp) op;
//                fsDir.unprotectedSetOwner(
//                        renameReservedPathsOnUpgrade(setOwnerOp.src, logVersion),
//                        setOwnerOp.username, setOwnerOp.groupname);
//                break;
//            }
//            case OP_RENAME: {
//                RenameOp renameOp = (RenameOp) op;
//                fsDir.unprotectedRenameTo(
//                        renameReservedPathsOnUpgrade(renameOp.src, logVersion),
//                        renameReservedPathsOnUpgrade(renameOp.dst, logVersion),
//                        renameOp.timestamp, renameOp.options);
//
//                if (toAddRetryCache) {
//                    fsNamesys.addCacheEntry(renameOp.rpcClientId, renameOp.rpcCallId);
//                }
//                break;
//            }
//      case OP_SET_GENSTAMP_V2: {
//        SetGenstampV2Op setGenstampV2Op = (SetGenstampV2Op) op;
//        fsNamesys.setGenerationStampV2(setGenstampV2Op.genStampV2);
//        break;
//      }
//            case OP_ALLOCATE_BLOCK_ID: {
//                AllocateBlockIdOp allocateBlockIdOp = (AllocateBlockIdOp) op;
//                fsNamesys.setLastAllocatedBlockId(allocateBlockIdOp.blockId);
//                break;
//            }
//      case OP_SET_ACL: {
//        SetAclOp setAclOp = (SetAclOp) op;
//        fsDir.unprotectedSetAcl(setAclOp.src, setAclOp.aclEntries, true);
//        break;
//      }
//      case OP_SET_XATTR: {
//        SetXAttrOp setXAttrOp = (SetXAttrOp) op;
//        fsDir.unprotectedSetXAttrs(setXAttrOp.src, setXAttrOp.xAttrs,
//                EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
//        if (toAddRetryCache) {
//          fsNamesys.addCacheEntry(setXAttrOp.rpcClientId, setXAttrOp.rpcCallId);
//        }
//        break;
//      }
//      case OP_REMOVE_XATTR: {
//        RemoveXAttrOp removeXAttrOp = (RemoveXAttrOp) op;
//        fsDir.unprotectedRemoveXAttrs(removeXAttrOp.src,
//                removeXAttrOp.xAttrs);
//        if (toAddRetryCache) {
//          fsNamesys.addCacheEntry(removeXAttrOp.rpcClientId,
//                  removeXAttrOp.rpcCallId);
//        }
//        break;
//      }
//      case OP_SET_STORAGE_POLICY: {
//        SetStoragePolicyOp setStoragePolicyOp = (SetStoragePolicyOp) op;
//        fsDir.unprotectedSetStoragePolicy(
//                renameReservedPathsOnUpgrade(setStoragePolicyOp.path, logVersion),
//                setStoragePolicyOp.policyId);
//        break;
//      }
            default:
                LOG.error("Invalid operation read " + op.opCode);
                break;
        }
        //return inodeId;
        return 0L;
    }


//    // allocate and update last allocated inode id
//    private long getAndUpdateLastInodeId(long inodeIdFromOp, int logVersion,
//                                         long lastInodeId) throws IOException {
//        long inodeId = inodeIdFromOp;
//
//        if (inodeId == INodeId.GRANDFATHER_INODE_ID) {
//            if (NameNodeLayoutVersion.supports(
//                    LayoutVersion.Feature.ADD_INODE_ID, logVersion)) {
//                throw new IOException("The layout version " + logVersion
//                        + " supports inodeId but gave bogus inodeId");
//            }
//            inodeId = fsNamesys.allocateNewInodeId();
//        } else {
//            // need to reset lastInodeId. fsnamesys gets lastInodeId firstly from
//            // fsimage but editlog captures more recent inodeId allocations
//            if (inodeId > lastInodeId) {
//                fsNamesys.resetLastInodeId(inodeId);
//            }
//        }
//        return inodeId;
//    }
//
//    private static String formatEditLogReplayError(EditLogInputStream in,
//                                                   long recentOpcodeOffsets[], long txid) {
//        StringBuilder sb = new StringBuilder();
//        sb.append("Error replaying edit log at offset " + in.getPosition());
//        sb.append(".  Expected transaction ID was ").append(txid);
//        if (recentOpcodeOffsets[0] != -1) {
//            Arrays.sort(recentOpcodeOffsets);
//            sb.append("\nRecent opcode offsets:");
//            for (long offset : recentOpcodeOffsets) {
//                if (offset != -1) {
//                    sb.append(' ').append(offset);
//                }
//            }
//        }
//        return sb.toString();
//    }
//
//    /**
//     * Add a new block into the given INodeFile
//     */
//    private void addNewBlock(FSDirectory fsDir, AddBlockOp op, INodeFile file)
//            throws IOException {
//        BlockInfo[] oldBlocks = file.getBlocks();
//        Block pBlock = op.getPenultimateBlock();
//        Block newBlock = op.getLastBlock();
//
//        if (pBlock != null) { // the penultimate block is not null
//            Preconditions.checkState(oldBlocks != null && oldBlocks.length > 0);
//            // compare pBlock with the last block of oldBlocks
//            Block oldLastBlock = oldBlocks[oldBlocks.length - 1];
//            if (oldLastBlock.getBlockId() != pBlock.getBlockId()
//                    || oldLastBlock.getGenerationStamp() != pBlock.getGenerationStamp()) {
//                throw new IOException(
//                        "Mismatched block IDs or generation stamps for the old last block of file "
//                                + op.getPath() + ", the old last block is " + oldLastBlock
//                                + ", and the block read from editlog is " + pBlock);
//            }
//
//            oldLastBlock.setNumBytes(pBlock.getNumBytes());
//            if (oldLastBlock instanceof BlockInfoUnderConstruction) {
//                fsNamesys.getBlockManager().forceCompleteBlock(file,
//                        (BlockInfoUnderConstruction) oldLastBlock);
//                fsNamesys.getBlockManager().processQueuedMessagesForBlock(pBlock);
//            }
//        } else { // the penultimate block is null
//            Preconditions.checkState(oldBlocks == null || oldBlocks.length == 0);
//        }
//        // add the new block
//        BlockInfo newBI = new BlockInfoUnderConstruction(
//                newBlock, file.getBlockReplication());
//        fsNamesys.getBlockManager().addBlockCollection(newBI, file);
//        file.addBlock(newBI);
//        fsNamesys.getBlockManager().processQueuedMessagesForBlock(newBlock);
//    }
//
//    /**
//     * Update in-memory data structures with new block information.
//     *
//     * @throws IOException
//     */
//    private void updateBlocks(FSDirectory fsDir, BlockListUpdatingOp op,
//                              INodeFile file) throws IOException {
//        // Update its block list
//        BlockInfo[] oldBlocks = file.getBlocks();
//        Block[] newBlocks = op.getBlocks();
//        String path = op.getPath();
//
//        // Are we only updating the last block's gen stamp.
//        boolean isGenStampUpdate = oldBlocks.length == newBlocks.length;
//
//        // First, update blocks in common
//        for (int i = 0; i < oldBlocks.length && i < newBlocks.length; i++) {
//            BlockInfo oldBlock = oldBlocks[i];
//            Block newBlock = newBlocks[i];
//
//            boolean isLastBlock = i == newBlocks.length - 1;
//            if (oldBlock.getBlockId() != newBlock.getBlockId() ||
//                    (oldBlock.getGenerationStamp() != newBlock.getGenerationStamp() &&
//                            !(isGenStampUpdate && isLastBlock))) {
//                throw new IOException("Mismatched block IDs or generation stamps, " +
//                        "attempting to replace block " + oldBlock + " with " + newBlock +
//                        " as block # " + i + "/" + newBlocks.length + " of " +
//                        path);
//            }
//
//            oldBlock.setNumBytes(newBlock.getNumBytes());
//            boolean changeMade =
//                    oldBlock.getGenerationStamp() != newBlock.getGenerationStamp();
//            oldBlock.setGenerationStamp(newBlock.getGenerationStamp());
//
//            if (oldBlock instanceof BlockInfoUnderConstruction &&
//                    (!isLastBlock || op.shouldCompleteLastBlock())) {
//                changeMade = true;
//                fsNamesys.getBlockManager().forceCompleteBlock(file,
//                        (BlockInfoUnderConstruction) oldBlock);
//            }
//            if (changeMade) {
//                // The state or gen-stamp of the block has changed. So, we may be
//                // able to process some messages from datanodes that we previously
//                // were unable to process.
//                fsNamesys.getBlockManager().processQueuedMessagesForBlock(newBlock);
//            }
//        }
//
//        if (newBlocks.length < oldBlocks.length) {
//            // We're removing a block from the file, e.g. abandonBlock(...)
//            if (!file.isUnderConstruction()) {
//                throw new IOException("Trying to remove a block from file " +
//                        path + " which is not under construction.");
//            }
//            if (newBlocks.length != oldBlocks.length - 1) {
//                throw new IOException("Trying to remove more than one block from file "
//                        + path);
//            }
//            Block oldBlock = oldBlocks[oldBlocks.length - 1];
//            boolean removed = fsDir.unprotectedRemoveBlock(path, file, oldBlock);
//            if (!removed && !(op instanceof UpdateBlocksOp)) {
//                throw new IOException("Trying to delete non-existant block " + oldBlock);
//            }
//        } else if (newBlocks.length > oldBlocks.length) {
//            // We're adding blocks
//            for (int i = oldBlocks.length; i < newBlocks.length; i++) {
//                Block newBlock = newBlocks[i];
//                BlockInfo newBI;
//                if (!op.shouldCompleteLastBlock()) {
//                    // TODO: shouldn't this only be true for the last block?
//                    // what about an old-version fsync() where fsync isn't called
//                    // until several blocks in?
//                    newBI = new BlockInfoUnderConstruction(
//                            newBlock, file.getBlockReplication());
//                } else {
//                    // OP_CLOSE should add finalized blocks. This code path
//                    // is only executed when loading edits written by prior
//                    // versions of Hadoop. Current versions always log
//                    // OP_ADD operations as each block is allocated.
//                    newBI = new BlockInfo(newBlock, file.getBlockReplication());
//                }
//                fsNamesys.getBlockManager().addBlockCollection(newBI, file);
//                file.addBlock(newBI);
//                fsNamesys.getBlockManager().processQueuedMessagesForBlock(newBlock);
//            }
//        }
//    }

    private static void dumpOpCounts(
            EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts) {
        StringBuilder sb = new StringBuilder();
        sb.append("Summary of operations loaded from edit log:\n  ");
        Joiner.on("\n  ").withKeyValueSeparator("=").appendTo(sb, opCounts);
        FSImage.LOG.debug(sb.toString());
    }

    private void incrOpCount(FSEditLogOpCodes opCode,
                             EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts, Step step,
                             Counter counter) {
        Holder<Integer> holder = opCounts.get(opCode);
        if (holder == null) {
            holder = new Holder<Integer>(1);
            opCounts.put(opCode, holder);
        } else {
            holder.held++;
        }
        counter.increment();
    }

    /**
     * Throw appropriate exception during upgrade from 203, when editlog loading
     * could fail due to opcode conflicts.
     */
    private void check203UpgradeFailure(int logVersion, Throwable e)
            throws IOException {
        // 0.20.203 version version has conflicting opcodes with the later releases.
        // The editlog must be emptied by restarting the namenode, before proceeding
        // with the upgrade.
        if (Storage.is203LayoutVersion(logVersion)
                && logVersion != HdfsConstants.NAMENODE_LAYOUT_VERSION) {
            String msg = "During upgrade failed to load the editlog version "
                    + logVersion + " from release 0.20.203. Please go back to the old "
                    + " release and restart the namenode. This empties the editlog "
                    + " and saves the namespace. Resume the upgrade after this step.";
            throw new IOException(msg, e);
        }
    }

    /**
     * Find the last valid transaction ID in the stream.
     * If there are invalid or corrupt transactions in the middle of the stream,
     * validateEditLog will skip over them.
     * This reads through the stream but does not close it.
     *
     * @param maxTxIdToValidate Maximum Tx ID to try to validate. Validation
     *                          returns after reading this or a higher ID.
     *                          The file portion beyond this ID is potentially
     *                          being updated.
     */
    static EditLogValidation validateEditLog(EditLogInputStream in,
                                             long maxTxIdToValidate) {
        long lastPos = 0;
        long lastTxId = HdfsConstants.INVALID_TXID;
        long numValid = 0;
        FSEditLogOp op = null;
        while (true) {
            lastPos = in.getPosition();
            try {
                if ((op = in.readOp()) == null) {
                    break;
                }
            } catch (Throwable t) {
                FSImage.LOG.warn("Caught exception after reading " + numValid +
                        " ops from " + in + " while determining its valid length." +
                        "Position was " + lastPos, t);
                in.resync();
                FSImage.LOG.warn("After resync, position is " + in.getPosition());
                continue;
            }
            if (lastTxId == HdfsConstants.INVALID_TXID
                    || op.getTransactionId() > lastTxId) {
                lastTxId = op.getTransactionId();
            }
            if (lastTxId >= maxTxIdToValidate) {
                break;
            }

            numValid++;
        }
        return new EditLogValidation(lastPos, lastTxId, false);
    }

    static EditLogValidation scanEditLog(EditLogInputStream in) {
        long lastPos = 0;
        long lastTxId = HdfsConstants.INVALID_TXID;
        long numValid = 0;
        FSEditLogOp op = null;
        while (true) {
            lastPos = in.getPosition();
            try {
                if ((op = in.readOp()) == null) { // TODO
                    break;
                }
            } catch (Throwable t) {
                FSImage.LOG.warn("Caught exception after reading " + numValid +
                        " ops from " + in + " while determining its valid length." +
                        "Position was " + lastPos, t);
                in.resync();
                FSImage.LOG.warn("After resync, position is " + in.getPosition());
                continue;
            }
            if (lastTxId == HdfsConstants.INVALID_TXID
                    || op.getTransactionId() > lastTxId) {
                lastTxId = op.getTransactionId();
            }
            numValid++;
        }
        return new EditLogValidation(lastPos, lastTxId, false);
    }

    static class EditLogValidation {
        private final long validLength;
        private final long endTxId;
        private final boolean hasCorruptHeader;

        EditLogValidation(long validLength, long endTxId,
                          boolean hasCorruptHeader) {
            this.validLength = validLength;
            this.endTxId = endTxId;
            this.hasCorruptHeader = hasCorruptHeader;
        }

        long getValidLength() {
            return validLength;
        }

        long getEndTxId() {
            return endTxId;
        }

        boolean hasCorruptHeader() {
            return hasCorruptHeader;
        }
    }

    /**
     * Stream wrapper that keeps track of the current stream position.
     * <p>
     * This stream also allows us to set a limit on how many bytes we can read
     * without getting an exception.
     */
    public static class PositionTrackingInputStream extends FilterInputStream
            implements StreamLimiter {
        private long curPos = 0;
        private long markPos = -1;
        private long limitPos = Long.MAX_VALUE;

        public PositionTrackingInputStream(InputStream is) {
            super(is);
        }

        private void checkLimit(long amt) throws IOException {
            long extra = (curPos + amt) - limitPos;
            if (extra > 0) {
                throw new IOException("Tried to read " + amt + " byte(s) past " +
                        "the limit at offset " + limitPos);
            }
        }

        @Override
        public int read() throws IOException {
            checkLimit(1);
            int ret = super.read();
            if (ret != -1) curPos++;
            return ret;
        }

        @Override
        public int read(byte[] data) throws IOException {
            checkLimit(data.length);
            int ret = super.read(data);
            if (ret > 0) curPos += ret;
            return ret;
        }

        @Override
        public int read(byte[] data, int offset, int length) throws IOException {
            checkLimit(length);
            int ret = super.read(data, offset, length);
            if (ret > 0) curPos += ret;
            return ret;
        }

        @Override
        public void setLimit(long limit) {
            limitPos = curPos + limit;
        }

        @Override
        public void clearLimit() {
            limitPos = Long.MAX_VALUE;
        }

        @Override
        public void mark(int limit) {
            super.mark(limit);
            markPos = curPos;
        }

        @Override
        public void reset() throws IOException {
            if (markPos == -1) {
                throw new IOException("Not marked!");
            }
            super.reset();
            curPos = markPos;
            markPos = -1;
        }

        public long getPos() {
            return curPos;
        }

        @Override
        public long skip(long amt) throws IOException {
            long extra = (curPos + amt) - limitPos;
            if (extra > 0) {
                throw new IOException("Tried to skip " + extra + " bytes past " +
                        "the limit at offset " + limitPos);
            }
            long ret = super.skip(amt);
            curPos += ret;
            return ret;
        }
    }

    public long getLastAppliedTxId() {
        return lastAppliedTxId;
    }

    /**
     * Creates a Step used for updating startup progress, populated with
     * information from the given edits.  The step always includes the log's name.
     * If the log has a known length, then the length is included in the step too.
     *
     * @param edits EditLogInputStream to use for populating step
     * @return Step populated with information from edits
     * @throws IOException thrown if there is an I/O error
     */
    private static Step createStartupProgressStep(EditLogInputStream edits)
            throws IOException {
        long length = edits.length();
        String name = edits.getCurrentStreamName();
        return length != -1 ? new Step(name, length) : new Step(name);
    }
}
