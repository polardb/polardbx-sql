/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.physicalbackfill;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.gms.partition.PhysicalBackfillDetailInfoFieldJSON;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.common.TddlConstants.LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@Deprecated
public class PhysicalBackfillExtractor {

    private long lastUpdateTime = 0l;
    protected final String schemaName;
    protected final String logicalTableName;
    protected final long batchSize;
    protected volatile long curSpeedLimit;
    protected final long parallelism;
    protected final long minUpdateBatch;
    protected final PhysicalBackfillManager backfillManager;
    protected final Map<String, Set<String>> sourcePhyTables;
    protected final Map<String, Set<String>> targetPhyTables;
    protected final Map<String, String> sourceTargetGroup;
    protected final boolean isBroadcast;
    protected final boolean newPartitionDb;

    protected final ITransactionManager tm;

    protected final PhysicalBackfillReporter reporter;
    private final Object lock = new Object();
    private final Map<String, String> groupStorageInsts = new TreeMap<>(String::compareToIgnoreCase);
    private final Map<String, Pair<String, String>> storageInstAndUserInfos = new ConcurrentHashMap<>();
    // key:target DN id, value:leader/follower host info
    private final Map<String, List<Pair<String, Integer>>> cacheTargetHostInfo =
        new TreeMap<>(String::compareToIgnoreCase);

    protected PhysicalBackfillExtractor(String schemaName, String logicalTableName,
                                        Map<String, Set<String>> sourcePhyTables,
                                        Map<String, Set<String>> targetPhyTables,
                                        Map<String, String> sourceTargetGroup,
                                        boolean isBroadcast,
                                        long batchSize, long parallelism,
                                        long minUpdateBatch) {
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.sourcePhyTables = sourcePhyTables;
        this.targetPhyTables = targetPhyTables;
        this.sourceTargetGroup = sourceTargetGroup;
        this.isBroadcast = isBroadcast;
        this.batchSize = batchSize;
        this.parallelism = parallelism;
        this.minUpdateBatch = minUpdateBatch;

        this.tm = ExecutorContext.getContext(schemaName).getTransactionManager();
        this.backfillManager = new PhysicalBackfillManager(schemaName);
        this.reporter = new PhysicalBackfillReporter(backfillManager);
        this.curSpeedLimit = OptimizerContext.getContext(schemaName).getParamManager()
            .getLong(ConnectionParams.PHYSICAL_BACKFILL_SPEED_LIMIT);
        this.newPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!newPartitionDb && GeneralUtil.isEmpty(sourceTargetGroup)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "missing source-target group mapping entry");
        }
    }

    /**
     * Load latest position mark
     *
     * @param ec Id of parent DDL job
     * @return this
     */
    public PhysicalBackfillManager.BackfillBean loadBackfillMeta(final ExecutionContext ec, final String dbIndex,
                                                                 final String phyTable,
                                                                 final String physicalPartition,
                                                                 final String sourceGroup, final String targetGroup,
                                                                 final Pair<String, String> srcFileAndDir,
                                                                 final Pair<String, String> targetFileAndDir,
                                                                 final long totalBatch, final long batchSize,
                                                                 final long offset, final long lsn,
                                                                 final Pair<String, Integer> sourceHost,
                                                                 final List<Pair<String, Integer>> targetHosts) {
        Long backfillId = ec.getBackfillId();

        // Init position mark with upper bound
        final PhysicalBackfillManager.BackfillObjectRecord initBfo =
            initUpperBound(backfillId, schemaName, logicalTableName, dbIndex, phyTable, physicalPartition, sourceGroup,
                targetGroup, srcFileAndDir, targetFileAndDir, totalBatch, batchSize, offset, lsn, sourceHost,
                targetHosts);

        // Insert ignore
        backfillManager.initBackfillMeta(backfillId, initBfo);

        // Load from system table
        PhysicalBackfillManager.BackfillBean backfillBean =
            this.reporter.loadBackfillMeta(backfillId, schemaName, dbIndex, phyTable, physicalPartition);

        SQLRecorderLogger.ddlLogger.info(
            String.format("loadBackfillMeta for backfillId %d: %s", backfillId, this.reporter.getBackfillBean()));

        return backfillBean;
    }

    public void insertBackfillMeta(final Long backfillId, final String dbIndex,
                                   final String phyTable,
                                   final String physicalPartition,
                                   final String sourceGroup, final String targetGroup,
                                   final Pair<String, String> srcFileAndDir,
                                   final Pair<String, String> targetFileAndDir,
                                   final long totalBatch, final long batchSize,
                                   final long offset, final long lsn,
                                   final Pair<String, Integer> sourceHost,
                                   final List<Pair<String, Integer>> targetHosts) {

        // Init position mark with upper bound
        final PhysicalBackfillManager.BackfillObjectRecord initBfo =
            initUpperBound(backfillId, schemaName, logicalTableName, dbIndex, phyTable, physicalPartition, sourceGroup,
                targetGroup, srcFileAndDir, targetFileAndDir, totalBatch, batchSize, offset, lsn, sourceHost,
                targetHosts);

        // Insert ignore
        backfillManager.initBackfillMeta(backfillId, initBfo);
    }

    private PhysicalBackfillManager.BackfillObjectRecord initUpperBound(final long ddlJobId, final String schemaName,
                                                                        final String tableName, final String dbIndex,
                                                                        final String phyTable,
                                                                        final String physicalPartition,
                                                                        final String sourceGroup,
                                                                        final String targetGroup,
                                                                        final Pair<String, String> srcFileAndDir,
                                                                        final Pair<String, String> targetFileAndDir,
                                                                        final long totalBatch, final long batchSize,
                                                                        final long offset, final long lsn,
                                                                        final Pair<String, Integer> sourceHost,
                                                                        final List<Pair<String, Integer>> targetHosts) {
        PhysicalBackfillManager.BackfillObjectRecord obj =
            getBackfillObjectRecords(ddlJobId, schemaName, tableName, dbIndex, phyTable, physicalPartition, sourceGroup,
                targetGroup, srcFileAndDir, targetFileAndDir, totalBatch, batchSize, offset, lsn);
        PhysicalBackfillDetailInfoFieldJSON json = new PhysicalBackfillDetailInfoFieldJSON();
        json.setTargetHostAndPorts(targetHosts);
        json.setSourceHostAndPort(sourceHost);
        obj.setDetailInfo(PhysicalBackfillDetailInfoFieldJSON.toJson(json));
        return obj;
    }

    @NotNull
    protected PhysicalBackfillManager.BackfillObjectRecord getBackfillObjectRecords(final long ddlJobId,
                                                                                    final String schemaName,
                                                                                    final String tableName,
                                                                                    final String physicalDb,
                                                                                    final String phyTable,
                                                                                    final String physicalPartition,
                                                                                    final String sourceGroup,
                                                                                    final String targetGroup,
                                                                                    final Pair<String, String> srcFileAndDir,
                                                                                    final Pair<String, String> targetFileAndDir,
                                                                                    final long totalBatch,
                                                                                    final long batchSize,
                                                                                    final long offset,
                                                                                    final long lsn) {
        return new PhysicalBackfillManager.BackfillObjectRecord(ddlJobId, schemaName, tableName, schemaName,
            tableName, physicalDb, phyTable, physicalPartition, sourceGroup, targetGroup, srcFileAndDir,
            targetFileAndDir, totalBatch, batchSize, offset, lsn);
    }

    public void doExtract(ExecutionContext ec, BatchConsumer batchConsumer) {
        PhysicalBackfillUtils.checkInterrupted(ec, null);
        groupStorageInsts.clear();
        storageInstAndUserInfos.clear();
        Map<String, Pair<String, String>> physicalTableGroupMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        AtomicBoolean alreadyWaitLsn = new AtomicBoolean(true);

        List<Pair<String, Pair<String, String>>> tablesInfo = new ArrayList<>();
        if (isBroadcast) {
            for (Map.Entry<String, Set<String>> sourceEntry : sourcePhyTables.entrySet()) {
                for (String sourcePhyTb : GeneralUtil.emptyIfNull(sourceEntry.getValue())) {
                    for (Map.Entry<String, Set<String>> targetEntry : targetPhyTables.entrySet()) {
                        assert GeneralUtil.emptyIfNull(targetEntry.getValue()).size() == 1;
                        for (String targetPhyTb : GeneralUtil.emptyIfNull(targetEntry.getValue())) {
                            tablesInfo.add(Pair.of(sourcePhyTb, Pair.of(sourceEntry.getKey(), targetEntry.getKey())));
                        }
                    }
                }
            }
        } else if (!newPartitionDb) {
            for (Map.Entry<String, Set<String>> sourceEntry : sourcePhyTables.entrySet()) {
                String targetGroup = sourceTargetGroup.get(sourceEntry.getKey());
                for (String sourcePhyTb : GeneralUtil.emptyIfNull(sourceEntry.getValue())) {
                    tablesInfo.add(Pair.of(sourcePhyTb, Pair.of(sourceEntry.getKey(), targetGroup)));
                }
            }
        } else {
            for (Map.Entry<String, Set<String>> sourceEntry : sourcePhyTables.entrySet()) {
                for (String sourcePhyTb : GeneralUtil.emptyIfNull(sourceEntry.getValue())) {
                    for (Map.Entry<String, Set<String>> targetEntry : targetPhyTables.entrySet()) {
                        if (targetEntry.getValue().contains(sourcePhyTb)) {
                            tablesInfo.add(Pair.of(sourcePhyTb, Pair.of(sourceEntry.getKey(), targetEntry.getKey())));
                        }
                    }
                }
            }
        }

        for (Pair<String, Pair<String, String>> tableInfo : tablesInfo) {
            String physicalTableName = tableInfo.getKey();
            String sourceGroupName = tableInfo.getValue().getKey();
            String targetGroupName = tableInfo.getValue().getValue();
            groupStorageInsts.putIfAbsent(sourceGroupName,
                DbTopologyManager.getStorageInstIdByGroupName(schemaName, sourceGroupName));
            groupStorageInsts.putIfAbsent(targetGroupName,
                DbTopologyManager.getStorageInstIdByGroupName(schemaName, targetGroupName));

            DbGroupInfoRecord srcDbGroupInfoRecord = ScaleOutPlanUtil.getDbGroupInfoByGroupName(sourceGroupName);
            DbGroupInfoRecord tarDbGroupInfoRecord = ScaleOutPlanUtil.getDbGroupInfoByGroupName(targetGroupName);

            assert srcDbGroupInfoRecord != null;
            assert tarDbGroupInfoRecord != null;

            Pair<String, String> srcDbAndGroup =
                Pair.of(srcDbGroupInfoRecord.phyDbName.toLowerCase(), srcDbGroupInfoRecord.groupName);
            Pair<String, String> targetDbAndGroup =
                Pair.of(tarDbGroupInfoRecord.phyDbName.toLowerCase(), tarDbGroupInfoRecord.groupName);

            List<String> phyPartNames =
                PhysicalBackfillUtils.getPhysicalPartitionNames(schemaName, srcDbAndGroup.getValue(),
                    srcDbAndGroup.getKey(),
                    physicalTableName);

            foreachPhysicalFile(ec, srcDbAndGroup, targetDbAndGroup, physicalTableName.toLowerCase(), phyPartNames,
                targetGroupName, alreadyWaitLsn, batchConsumer);
        }
    }

    public void foreachPhysicalFile(final ExecutionContext ec, final Pair<String, String> srcDbAndGroup,
                                    final Pair<String, String> targetDbAndGroup, final String phyTable,
                                    final List<String> physicalPartNames, final String targetGroupName,
                                    final AtomicBoolean alreadyWaitLsn, final BatchConsumer consumer) {

        //1 flush table for export FLUSH TABLES t1 FOR EXPORT;
        //2 copy ibd
        //3 unlock table
        //4 upsert fileInfo to gms
        //5 copy to target dn
        //6 delete temp ibd file

        String msg = "begin to backfill the idb file for table[" + srcDbAndGroup.getKey() + ":" + phyTable + "]";
        SQLRecorderLogger.ddlLogger.info(msg);

        DbGroupInfoRecord tarDbGroupInfoRecord = ScaleOutPlanUtil.getDbGroupInfoByGroupName(targetGroupName);

        boolean hasNoPhyPart = GeneralUtil.isEmpty(physicalPartNames);
        if (hasNoPhyPart) {
            physicalPartNames.add("");
        }
        String sourceStorageInstId = groupStorageInsts.get(srcDbAndGroup.getValue());
        String targetStorageInstId = groupStorageInsts.get(targetDbAndGroup.getValue());
        Pair<String, String> userInfo = storageInstAndUserInfos.computeIfAbsent(sourceStorageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(sourceStorageInstId));

        boolean healthyCheck =
            ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK);

        for (String physicalPartition : physicalPartNames) {
            List<Pair<Long, Long>> offsetAndSize = new ArrayList<>();

            final Pair<String, String> targetFileAndDir;

            PhysicalBackfillManager.BackfillBean initBean =
                reporter.loadBackfillMeta(ec.getBackfillId(), schemaName, srcDbAndGroup.getKey(), phyTable,
                    physicalPartition);
            Pair<String, String> srcFileAndDir = null;
            Long lsn = 0l;
            Pair<String, Integer> sourceHost = null;
            final Pair<String, String> tempFileAndDir;
            List<Pair<String, Integer>> targetHosts = null;
            if (initBean.isEmpty() || initBean.isInit()) {
                final Boolean copyIbdFromFollower =
                    ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_FROM_FOLLOWER);

                if (copyIbdFromFollower) {
                    sourceHost = PhysicalBackfillUtils.getMySQLOneFollowerIpAndPort(sourceStorageInstId);
                } else {
                    sourceHost = PhysicalBackfillUtils.getMySQLLeaderIpAndPort(sourceStorageInstId);
                }
                //cache&recheck targetHost for per DN, in the same backfill task, the targethost can't HA
                targetHosts = cacheTargetHostInfo.computeIfAbsent(targetStorageInstId,
                    key -> PhysicalBackfillUtils.getMySQLServerNodeIpAndPorts(targetStorageInstId, healthyCheck));
                Map<String, String> targetGroupAndStorageIdMap = new HashMap<>();
                Map<String, String> sourceGroupAndStorageIdMap = new HashMap<>();

                targetGroupAndStorageIdMap.put(targetDbAndGroup.getValue(), targetStorageInstId);
                sourceGroupAndStorageIdMap.put(srcDbAndGroup.getValue(), sourceStorageInstId);

                if (!alreadyWaitLsn.get()) {

                    //wait target DN to finish the ddl
                    Map<String, Long> groupAndLsnMap =
                        PhysicalBackfillUtils.waitLsn(schemaName, targetGroupAndStorageIdMap, false, ec);
                    //wait source DN to replay the dml binlog in follower
                    PhysicalBackfillUtils.waitLsn(schemaName, sourceGroupAndStorageIdMap, false, ec);
                    lsn = groupAndLsnMap.get(targetDbAndGroup.getValue());
                    alreadyWaitLsn.set(true);
                }
                Map<String, Pair<String, String>> srcFileAndDirs =
                    PhysicalBackfillUtils.getSourceTableInfo(userInfo, srcDbAndGroup.getKey(), phyTable,
                        physicalPartNames,
                        hasNoPhyPart, sourceHost);
                if (!initBean.isEmpty()) {
                    for (String phyPart : physicalPartNames) {
                        PhysicalBackfillManager.BackfillBean backfillBean =
                            reporter.loadBackfillMeta(ec.getBackfillId(), schemaName, srcDbAndGroup.getKey(), phyTable,
                                phyPart);
                        assert backfillBean.isInit();
                        PhysicalBackfillManager.BackfillObjectBean bean = backfillBean.backfillObject;
                        try {
                            PhysicalBackfillDetailInfoFieldJSON detailInfoFieldJSON = bean.detailInfo;
                            PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, bean.sourceGroupName,
                                bean.physicalDb,
                                detailInfoFieldJSON.getSourceHostAndPort().getKey(),
                                detailInfoFieldJSON.getSourceHostAndPort().getValue(),
                                PhysicalBackfillUtils.convertToCfgFileName(bean.sourceDirName,
                                    PhysicalBackfillUtils.CFG), true, ec);
                            PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, bean.sourceGroupName,
                                bean.physicalDb,
                                detailInfoFieldJSON.getSourceHostAndPort().getKey(),
                                detailInfoFieldJSON.getSourceHostAndPort().getValue(), bean.sourceDirName, true, ec);
                        } catch (Exception ex) {
                            //ignore
                            try {
                                SQLRecorderLogger.ddlLogger.info(ex.toString());
                            } catch (Exception e) {

                            }
                        }
                        backfillManager.deleteById(bean.id);
                    }
                }
                for (Map.Entry<String, Pair<String, String>> entry : srcFileAndDirs.entrySet()) {
                    Pair<String, String> partSrcFileAndDir = entry.getValue();
                    String tmpDir = partSrcFileAndDir.getValue() + PhysicalBackfillUtils.TEMP_FILE_POSTFIX;
                    String tmpFile = partSrcFileAndDir.getKey();
                    Pair<String, String> partTempFileAndDir = Pair.of(tmpFile, tmpDir);

                    String partTargetFile = partSrcFileAndDir.getKey().substring(srcDbAndGroup.getKey().length());
                    partTargetFile = tarDbGroupInfoRecord.phyDbName.toLowerCase() + partTargetFile;

                    String partTargetDir = partSrcFileAndDir.getValue()
                        .substring(PhysicalBackfillUtils.IDB_DIR_PREFIX.length() + srcDbAndGroup.getKey().length());
                    partTargetDir = PhysicalBackfillUtils.IDB_DIR_PREFIX + tarDbGroupInfoRecord.phyDbName.toLowerCase()
                        + partTargetDir;

                    Pair<String, String> partTargetFileAndDir = new Pair<>(partTargetFile, partTargetDir);

                    insertBackfillMeta(ec.getBackfillId(), srcDbAndGroup.getKey(), phyTable, entry.getKey(),
                        srcDbAndGroup.getValue(),
                        targetDbAndGroup.getValue(), partTempFileAndDir, partTargetFileAndDir, 0,
                        batchSize, 0, lsn, sourceHost, targetHosts);

                }
                cloneInnodbDataFile(ec, srcDbAndGroup, srcFileAndDirs, phyTable, physicalPartNames, hasNoPhyPart,
                    sourceHost);
                for (Map.Entry<String, Pair<String, String>> entry : srcFileAndDirs.entrySet()) {

                    PhysicalBackfillUtils.getTempIbdFileInfo(userInfo, sourceHost, srcDbAndGroup, phyTable,
                        entry.getKey(), entry.getValue(),
                        batchSize, false, offsetAndSize);

                    PhysicalBackfillManager.BackfillBean backfillBean =
                        reporter.loadBackfillMeta(ec.getBackfillId(), schemaName, srcDbAndGroup.getKey(), phyTable,
                            entry.getKey());
                    assert backfillBean.isInit();

                    backfillManager.updateStatusAndTotalBatch(backfillBean.backfillObject.id, offsetAndSize.size());
                }
                if (!hasNoPhyPart) {
                    srcFileAndDir = srcFileAndDirs.get(physicalPartition);
                } else {
                    srcFileAndDir = srcFileAndDirs.entrySet().iterator().next().getValue();
                }
                tempFileAndDir =
                    PhysicalBackfillUtils.getTempIbdFileInfo(userInfo, sourceHost, srcDbAndGroup, phyTable,
                        physicalPartition, srcFileAndDir, batchSize,
                        false, offsetAndSize);

                String partTargetFile = srcFileAndDir.getKey().substring(srcDbAndGroup.getKey().length());
                partTargetFile = tarDbGroupInfoRecord.phyDbName.toLowerCase() + partTargetFile;

                String partTargetDir =
                    srcFileAndDir.getValue()
                        .substring(PhysicalBackfillUtils.IDB_DIR_PREFIX.length() + srcDbAndGroup.getKey().length());
                partTargetDir =
                    PhysicalBackfillUtils.IDB_DIR_PREFIX + tarDbGroupInfoRecord.phyDbName.toLowerCase() + partTargetDir;

                targetFileAndDir = new Pair<>(partTargetFile, partTargetDir);

            } else {
                if (initBean.isSuccess()) {
                    continue;
                }
                sourceHost = initBean.backfillObject.detailInfo.getSourceHostAndPort();
                srcFileAndDir = Pair.of(initBean.backfillObject.sourceFileName, initBean.backfillObject.sourceDirName);
                tempFileAndDir = srcFileAndDir;
                targetFileAndDir =
                    Pair.of(initBean.backfillObject.targetFileName, initBean.backfillObject.targetDirName);
                //update the offsetAndSize
                PhysicalBackfillUtils.getTempIbdFileInfo(userInfo, sourceHost, srcDbAndGroup, phyTable,
                    physicalPartition, srcFileAndDir, batchSize,
                    true, offsetAndSize);
            }

            BitSet bitSet;
            long[] bitSetPosMark = null;
            PhysicalBackfillManager.BackfillBean backfillBean =
                loadBackfillMeta(ec, srcDbAndGroup.getKey(), phyTable, physicalPartition, srcDbAndGroup.getValue(),
                    targetDbAndGroup.getValue(), tempFileAndDir, targetFileAndDir, offsetAndSize.size(), batchSize, 0,
                    lsn, sourceHost, targetHosts);

            assert !backfillBean.isInit();

            PhysicalBackfillDetailInfoFieldJSON detailInfo = backfillBean.backfillObject.detailInfo;
            if (backfillBean.isSuccess()) {
                return;
            } else {
                if (detailInfo != null) {
                    bitSetPosMark = detailInfo.getBitSet();
                } else {
                    detailInfo = new PhysicalBackfillDetailInfoFieldJSON();
                }
            }

            List<Future> futures = new ArrayList<>(16);
            AtomicReference<Exception> excep = new AtomicReference<>(null);
            final AtomicInteger successBatch = new AtomicInteger(0);
            final List<Pair<String, Integer>> targetHost = detailInfo.getTargetHostAndPorts();
            final Pair<String, Integer> sourceHostIpAndPort = detailInfo.getSourceHostAndPort();
            final AtomicReference<Boolean> interrupted = new AtomicReference<>(false);

            // copy the .cfg file before .ibd file
            String srcFileName = srcFileAndDir.getKey();
            String srcDir;
            if (initBean.isEmpty()) {
                srcDir = PhysicalBackfillUtils.convertToCfgFileName(
                    srcFileAndDir.getValue() + PhysicalBackfillUtils.TEMP_FILE_POSTFIX, PhysicalBackfillUtils.CFG);
            } else {
                srcDir =
                    PhysicalBackfillUtils.convertToCfgFileName(srcFileAndDir.getValue(), PhysicalBackfillUtils.CFG);
            }

            String tarFileName = targetFileAndDir.getKey();
            String tarDir =
                PhysicalBackfillUtils.convertToCfgFileName(targetFileAndDir.getValue(), PhysicalBackfillUtils.CFG);
            copyCfgFile(Pair.of(srcFileName, srcDir), srcDbAndGroup, sourceHostIpAndPort,
                Pair.of(tarFileName, tarDir), targetDbAndGroup, targetHost, consumer);

            if (bitSetPosMark == null || bitSetPosMark.length == 0) {
                bitSet = new BitSet(offsetAndSize.size());
            } else {
                bitSet = BitSet.valueOf(bitSetPosMark);
            }

            long fileSize = 0l;
            if (offsetAndSize.size() > 0) {
                Pair<Long, Long> lastBatch = offsetAndSize.get(offsetAndSize.size() - 1);
                fileSize = lastBatch.getKey() + lastBatch.getValue();
            }
            fallocateIbdFile(ec, targetFileAndDir, targetDbAndGroup, targetHost, phyTable, "", fileSize);

            if (parallelism <= 0 || parallelism >= BackFillThreadPool.getInstance().getCorePoolSize()) {
                // Full queued.
                offsetAndSize.forEach(v -> {
                    int index = (int) (v.getKey() / batchSize);
                    if (!bitSet.get(index)) {
                        FutureTask<Void> task = new FutureTask<>(
                            () -> foreachPhyFileBatch(srcDbAndGroup, targetDbAndGroup, tempFileAndDir, targetFileAndDir,
                                phyTable, v, bitSet, batchSize, successBatch, minUpdateBatch, sourceHostIpAndPort,
                                targetHost, consumer, ec, interrupted), null);
                        futures.add(task);
                        BackFillThreadPool.getInstance()
                            .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_BACKFILL_TASK);
                    }
                });
            } else {

                // Use a bounded blocking queue to control the parallelism.
                BlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>((int) parallelism);

                offsetAndSize.forEach(v -> {
                    int index = (int) (v.getKey() / batchSize);
                    if (!bitSet.get(index)) {
                        if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()
                            || interrupted.get()) {
                            long jobId = ec.getDdlJobId();
                            excep.set(new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                "The job '" + jobId + "' has been cancelled"));
                            interrupted.set(true);
                            return;
                        } else {
                            try {
                                blockingQueue.put(new Object());
                            } catch (Exception e) {
                                excep.set(e);
                                interrupted.set(true);
                            }
                        }
                        if (null == excep.get() && !interrupted.get()) {
                            FutureTask<Void> task = new FutureTask<>(() -> {
                                try {
                                    foreachPhyFileBatch(srcDbAndGroup, targetDbAndGroup, tempFileAndDir,
                                        targetFileAndDir, phyTable, v, bitSet, batchSize, successBatch, minUpdateBatch,
                                        sourceHostIpAndPort, targetHost, consumer, ec, interrupted);
                                } finally {
                                    // Poll in finally to prevent dead lock on putting blockingQueue.
                                    blockingQueue.poll();
                                }
                                return null;
                            });
                            futures.add(task);
                            BackFillThreadPool.getInstance()
                                .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_BACKFILL_TASK);
                        }
                    }
                });
            }

            if (excep.get() != null) {
                // Interrupt all.
                futures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignore) {
                    }
                });
            }

            for (Future future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    futures.forEach(f -> {
                        try {
                            f.cancel(true);
                        } catch (Throwable ignore) {
                        }
                    });
                    if (null == excep.get()) {
                        excep.set(e);
                    }
                    // set interrupt
                    interrupted.set(true);
                }
            }

            PhysicalBackfillManager.BackfillBean bfb = reporter.getBackfillBean();
            PhysicalBackfillManager.BackfillObjectRecord bor = new PhysicalBackfillManager.BackfillObjectRecord();
            bor.setJobId(bfb.backfillObject.jobId);
            bor.setSuccessBatchCount(bfb.backfillObject.successBatchCount + successBatch.get());
            bor.setExtra(bfb.backfillObject.extra);
            bor.setPhysicalDb(bfb.backfillObject.physicalDb);
            bor.setPhysicalTable(bfb.backfillObject.physicalTable);
            bor.setPhysicalPartition(bfb.backfillObject.physicalPartition);
            bor.setEndTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()));

            if (excep.get() != null) {
                detailInfo.setMsg(excep.get().toString());
                detailInfo.setBitSet(bitSet.toLongArray());
                bor.setDetailInfo(PhysicalBackfillDetailInfoFieldJSON.toJson(detailInfo));
                bor.setStatus((int) PhysicalBackfillManager.BackfillStatus.FAILED.getValue());

                reporter.updateBackfillObject(bor);
                throw GeneralUtil.nestedException(excep.get());
            }
            bfb.backfillObject.detailInfo.setBitSet(null);
            bor.setStatus((int) PhysicalBackfillManager.BackfillStatus.SUCCESS.getValue());
            bor.setDetailInfo(PhysicalBackfillDetailInfoFieldJSON.toJson(bfb.backfillObject.detailInfo));
            bor.setSuccessBatchCount(offsetAndSize.size());

            Pair<String, Integer> ipPortPair = bfb.backfillObject.detailInfo.getSourceHostAndPort();

            PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, srcDbAndGroup.getValue(), srcDbAndGroup.getKey(),
                ipPortPair.getKey(), ipPortPair.getValue(),
                PhysicalBackfillUtils.convertToCfgFileName(tempFileAndDir.getValue(), PhysicalBackfillUtils.CFG), false,
                ec);

            PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, srcDbAndGroup.getValue(), srcDbAndGroup.getKey(),
                ipPortPair.getKey(), ipPortPair.getValue(), tempFileAndDir.getValue(), false, ec);
            // After all physical table finished
            reporter.updateBackfillObject(bor);
        }
        msg = "already backfill the idb file for table[" + srcDbAndGroup.getKey() + ":" + phyTable + "]";
        SQLRecorderLogger.ddlLogger.info(msg);
    }

    private void foreachPhyFileBatch(final Pair<String, String> srcDbAndGroup,
                                     final Pair<String, String> targetDbAndGroup,
                                     final Pair<String, String> srcFileAndDir,
                                     final Pair<String, String> targetFileAndDir,
                                     final String physicalTableName,
                                     final Pair<Long, Long> offsetAndSize,
                                     final BitSet bitSet,
                                     long batchSize,
                                     final AtomicInteger successBatch,
                                     final long minUpdateBatch,
                                     final Pair<String, Integer> sourceHost,
                                     final List<Pair<String, Integer>> targetHost,
                                     final BatchConsumer consumer,
                                     final ExecutionContext ec,
                                     final AtomicReference<Boolean> interrupted) {
        String sourceStorageInstId = groupStorageInsts.get(srcDbAndGroup.getValue());
        String targetStorageInstId = groupStorageInsts.get(targetDbAndGroup.getValue());

        PolarxPhysicalBackfill.TransferFileDataOperator transferFileData = null;

        Pair<String, String> srcUserInfo = storageInstAndUserInfos.computeIfAbsent(sourceStorageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(sourceStorageInstId));
        Pair<String, String> tarUserInfo = storageInstAndUserInfos.computeIfAbsent(sourceStorageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(targetStorageInstId));

        boolean success = false;
        int tryTime = 1;
        DecimalFormat df = new DecimalFormat("#.0");

        Long speedLimit = OptimizerContext.getContext(schemaName).getParamManager()
            .getLong(ConnectionParams.PHYSICAL_BACKFILL_SPEED_LIMIT);
        if (speedLimit.longValue() != PhysicalBackfillUtils.getRateLimiter().getCurSpeedLimiter()) {
            this.curSpeedLimit = speedLimit;
            if (speedLimit > 0) {
                double curSpeed = PhysicalBackfillUtils.getRateLimiter().getRate() / 1024;
                PhysicalBackfillUtils.getRateLimiter().setRate(speedLimit.longValue());
                String msg =
                    "change the maximum speed limit from " + df.format(curSpeed) + "KB/s to "
                        + df.format(PhysicalBackfillUtils.getRateLimiter().getRate() / 1024)
                        + "KB/s";
                SQLRecorderLogger.ddlLogger.info(msg);
            }
        }
        do {
            // Check DDL is ongoing.
            PhysicalBackfillUtils.checkInterrupted(ec, interrupted);
            if (this.curSpeedLimit > 0) {
                PhysicalBackfillUtils.getRateLimiter().acquire(offsetAndSize.getValue().intValue());
            }
            try (
                XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(srcDbAndGroup.getKey(),
                    sourceHost.getKey(), sourceHost.getValue(), srcUserInfo.getKey(), srcUserInfo.getValue(), -1))) {
                PolarxPhysicalBackfill.TransferFileDataOperator.Builder builder =
                    PolarxPhysicalBackfill.TransferFileDataOperator.newBuilder();

                builder.setOperatorType(PolarxPhysicalBackfill.TransferFileDataOperator.Type.GET_DATA_FROM_SRC_IBD);
                PolarxPhysicalBackfill.FileInfo.Builder fileInfoBuilder = PolarxPhysicalBackfill.FileInfo.newBuilder();
                fileInfoBuilder.setFileName(srcFileAndDir.getKey());
                fileInfoBuilder.setTempFile(false);
                fileInfoBuilder.setDirectory(srcFileAndDir.getValue());
                fileInfoBuilder.setPartitionName("");
                builder.setFileInfo(fileInfoBuilder.build());
                builder.setBufferLen(offsetAndSize.getValue());
                builder.setOffset(offsetAndSize.getKey());
                transferFileData = conn.execReadBufferFromFile(builder);
                success = true;
            } catch (Exception ex) {
                if (tryTime >= PhysicalBackfillUtils.MAX_RETRY) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                }
                tryTime++;
            }
        } while (!success);
        consumer.consume(targetDbAndGroup, targetFileAndDir, targetHost, tarUserInfo,
            transferFileData);
        synchronized (lock) {
            if (lastUpdateTime == 0) {
                lastUpdateTime = System.currentTimeMillis();
            }
            bitSet.set((int) (transferFileData.getOffset() / batchSize));
            int curSuccessBatch = successBatch.incrementAndGet();
            if (curSuccessBatch >= minUpdateBatch) {
                long curTime = System.currentTimeMillis();
                //update to metadb
                PhysicalBackfillDetailInfoFieldJSON detailInfo = new PhysicalBackfillDetailInfoFieldJSON();
                detailInfo.setBitSet(bitSet.toLongArray());
                detailInfo.setMsg("");

                PhysicalBackfillManager.BackfillBean bfb = reporter.getBackfillBean();
                PhysicalBackfillManager.BackfillObjectRecord bor =
                    new PhysicalBackfillManager.BackfillObjectRecord();

                detailInfo.setSourceHostAndPort(bfb.backfillObject.detailInfo.sourceHostAndPort);
                detailInfo.setTargetHostAndPorts(bfb.backfillObject.detailInfo.targetHostAndPorts);

                bor.setJobId(bfb.backfillObject.jobId);
                bor.setSuccessBatchCount(bfb.backfillObject.successBatchCount + successBatch.get());
                bor.setExtra(bfb.backfillObject.extra);
                bor.setPhysicalDb(bfb.backfillObject.physicalDb);
                bor.setPhysicalTable(bfb.backfillObject.physicalTable);
                bor.setPhysicalPartition(bfb.backfillObject.physicalPartition);
                bor.setEndTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()));
                bor.setStatus((int) PhysicalBackfillManager.BackfillStatus.RUNNING.getValue());
                bor.setDetailInfo(PhysicalBackfillDetailInfoFieldJSON.toJson(detailInfo));

                reporter.updateBackfillObject(bor);
                reporter.loadBackfillMeta(bor.getJobId(), bor.getTableSchema(), bor.getPhysicalDb(),
                    bor.getPhysicalTable(),
                    bor.getPhysicalPartition());

                successBatch.set(0);

                double speed = (curSuccessBatch * batchSize) * 1000.0 / Math.max(1, curTime - lastUpdateTime) / 1024;

                //todo calc the speed by 1000 batch / time
                String msg = "already write " + curSuccessBatch + " batch successfully for " + srcFileAndDir.getValue()
                    + " speed:" + df.format(speed) + "KB/s the maximum speed limit:"
                    + df.format(PhysicalBackfillUtils.getRateLimiter().getRate() / 1024) + "KB/s";
                SQLRecorderLogger.ddlLogger.info(msg);
                lastUpdateTime = System.currentTimeMillis();
            }
        }
    }

    private void cloneInnodbDataFile(final ExecutionContext ec, final Pair<String, String> dbAndGroup,
                                     final Map<String, Pair<String, String>> srcFileAndDirs, String phyTableName,
                                     List<String> phyPartNames, boolean hasNoPhyPart,
                                     Pair<String, Integer> sourceIpAndPort) {

        String sourceStorageInstId = groupStorageInsts.get(dbAndGroup.getValue());

        String msg = "begin to clone the files for table:" + phyTableName;
        SQLRecorderLogger.ddlLogger.info(msg);
        XConnection conn = null;
        Pair<String, String> userInfo = storageInstAndUserInfos.computeIfAbsent(sourceStorageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(sourceStorageInstId));
        boolean success = false;
        int tryTime = 1;
        StringBuilder copyFileInfo = null;
        AtomicReference<Boolean> finished = new AtomicReference<>(false);
        do {
            try {
                copyFileInfo = new StringBuilder();
                conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(dbAndGroup.getKey(),
                    sourceIpAndPort.getKey(), sourceIpAndPort.getValue(), userInfo.getKey(), userInfo.getValue(), -1));
                conn.setNetworkTimeoutNanos(LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN * 1000000L);
                conn.execQuery(String.format(PhysicalBackfillUtils.FLUSH_TABLE_SQL_TEMPLATE, phyTableName));
                PolarxPhysicalBackfill.FileManageOperator.Builder builder =
                    PolarxPhysicalBackfill.FileManageOperator.newBuilder();

                PolarxPhysicalBackfill.TableInfo.Builder tableInfoBuilder =
                    PolarxPhysicalBackfill.TableInfo.newBuilder();
                tableInfoBuilder.setTableSchema(dbAndGroup.getKey());
                tableInfoBuilder.setTableName(phyTableName);
                tableInfoBuilder.setPartitioned(!hasNoPhyPart);
                int i = 0;
                for (Map.Entry<String, Pair<String, String>> entry : srcFileAndDirs.entrySet()) {
                    Pair<String, String> srcFileAndDir = entry.getValue();

                    boolean handlerIbdFile = false;
                    do {
                        PhysicalBackfillUtils.checkInterrupted(ec, null);
                        PolarxPhysicalBackfill.FileInfo.Builder srcFileInfoBuilder =
                            PolarxPhysicalBackfill.FileInfo.newBuilder();

                        String fileName = srcFileAndDir.getKey();
                        String directory = srcFileAndDir.getValue();

                        if (!handlerIbdFile) {
                            directory =
                                PhysicalBackfillUtils.convertToCfgFileName(directory, PhysicalBackfillUtils.CFG);
                        }

                        srcFileInfoBuilder.setFileName(fileName);
                        srcFileInfoBuilder.setDirectory(directory);
                        srcFileInfoBuilder.setPartitionName(entry.getKey());

                        PolarxPhysicalBackfill.FileInfo.Builder tmpFileInfoBuilder =
                            PolarxPhysicalBackfill.FileInfo.newBuilder();
                        tmpFileInfoBuilder.setFileName(fileName);
                        tmpFileInfoBuilder.setDirectory(directory + PhysicalBackfillUtils.TEMP_FILE_POSTFIX);
                        tmpFileInfoBuilder.setPartitionName(entry.getKey());

                        tableInfoBuilder.addFileInfo(srcFileInfoBuilder.build());
                        tableInfoBuilder.addFileInfo(tmpFileInfoBuilder.build());
                        if (i > 0) {
                            copyFileInfo.append(", ");
                        }
                        copyFileInfo.append(directory);
                        i++;
                        if (handlerIbdFile) {
                            break;
                        } else {
                            handlerIbdFile = true;
                        }
                    } while (true);
                }
                builder.setTableInfo(tableInfoBuilder.build());

                builder.setOperatorType(PolarxPhysicalBackfill.FileManageOperator.Type.COPY_IBD_TO_TEMP_DIR_IN_SRC);

                Thread parentThread = Thread.currentThread();
                XSession session = conn.getSession();
                finished.set(false);
                FutureTask<Void> task = new FutureTask<>(() -> {
                    do {
                        if (finished.get()) {
                            break;
                        }
                        if (session == null) {
                            SQLRecorderLogger.ddlLogger.info("exeCloneFile session was terminated");
                            break;
                        }
                        if (parentThread.isInterrupted() || CrossEngineValidator.isJobInterrupted(ec)) {
                            SQLRecorderLogger.ddlLogger.info(
                                String.format("exeCloneFile session was cancel, sessionId:%d", session.getSessionId()));
                            session.cancel();
                            break;
                        }
                        try {
                            Thread.sleep(100);
                        } catch (Exception e) {
                            //ignore
                        }
                    } while (true);
                }, null);
                Future futureTask =
                    ec.getExecutorService().submit(ec.getSchemaName(), ec.getTraceId(), AsyncTask.build(task));

                conn.exeCloneFile(builder);

                finished.set(true);
                try {
                    futureTask.get();
                } catch (Exception ex) {
                    try {
                        futureTask.cancel(true);
                    } catch (Throwable ignore) {
                    }
                }
                msg = String.format("already clone the files[%s] for table %s", copyFileInfo, phyTableName);
                SQLRecorderLogger.ddlLogger.info(msg);
                success = true;
            } catch (Exception ex) {
                msg = String.format("fail to clone those files:%s, [ip:%s,port:%s,db:%s]", copyFileInfo.toString(),
                    sourceIpAndPort.getKey(), sourceIpAndPort.getValue().toString(), dbAndGroup.getKey());
                if (ex != null && ex.toString() != null) {
                    msg += " " + ex.toString();
                }
                SQLRecorderLogger.ddlLogger.info(msg);
                if (tryTime > PhysicalBackfillUtils.MAX_RETRY) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                }
                PhysicalBackfillUtils.checkInterrupted(ec, null);
                tryTime++;
            } finally {
                try {
                    finished.set(true);
                    if (conn != null && !conn.isClosed()) {
                        try {
                            conn.execQuery(PhysicalBackfillUtils.UNLOCK_TABLE);
                        } catch (SQLException e) {
                            msg = "fail to clone those files:" + copyFileInfo.toString() + " " + e.toString();
                            SQLRecorderLogger.ddlLogger.info(msg);
                            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, e);
                        }
                    }
                } catch (SQLException ex) {
                    msg = "fail to clone those files:" + copyFileInfo.toString() + " " + ex.toString();
                    SQLRecorderLogger.ddlLogger.info(msg);
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                }
            }
        } while (!success);
    }

    private void fallocateIbdFile(final ExecutionContext ec, final Pair<String, String> targetFileAndDir,
                                  final Pair<String, String> tarDbAndGroup,
                                  final List<Pair<String, Integer>> targetHosts, String physicalTableName,
                                  String phyPartitionName, long fileSize) {
        String msg = "begin to fallocate ibd file:" + targetFileAndDir.getValue();
        SQLRecorderLogger.ddlLogger.info(msg);

        String tarStorageInstId = groupStorageInsts.get(tarDbAndGroup.getValue());

        PolarxPhysicalBackfill.GetFileInfoOperator getFileInfoOperator = null;
        Pair<String, String> tempFileAndDir = null;
        Pair<String, String> userInfo = storageInstAndUserInfos.computeIfAbsent(tarStorageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(tarStorageInstId));

        for (Pair<String, Integer> targetHost : targetHosts) {
            boolean success = false;
            int tryTime = 1;
            do {
                PhysicalBackfillUtils.checkInterrupted(ec, null);
                try (XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(
                    tarDbAndGroup.getKey(),
                    targetHost.getKey(), targetHost.getValue(), userInfo.getKey(), userInfo.getValue(), -1))) {
                    PolarxPhysicalBackfill.FileManageOperator.Builder builder =
                        PolarxPhysicalBackfill.FileManageOperator.newBuilder();

                    builder.setOperatorType(PolarxPhysicalBackfill.FileManageOperator.Type.FALLOCATE_IBD);
                    PolarxPhysicalBackfill.TableInfo.Builder tableInfoBuilder =
                        PolarxPhysicalBackfill.TableInfo.newBuilder();
                    tableInfoBuilder.setTableSchema(tarDbAndGroup.getKey());
                    tableInfoBuilder.setTableName(physicalTableName);
                    tableInfoBuilder.setPartitioned(false);

                    PolarxPhysicalBackfill.FileInfo.Builder fileInfoBuilder =
                        PolarxPhysicalBackfill.FileInfo.newBuilder();
                    fileInfoBuilder.setTempFile(false);
                    fileInfoBuilder.setFileName(targetFileAndDir.getKey());
                    fileInfoBuilder.setPartitionName(phyPartitionName);
                    fileInfoBuilder.setDirectory(targetFileAndDir.getValue());
                    fileInfoBuilder.setDataSize(fileSize);

                    tableInfoBuilder.addFileInfo(fileInfoBuilder.build());
                    builder.setTableInfo(tableInfoBuilder.build());

                    conn.execFallocateIbdFile(builder);
                    success = true;
                } catch (Exception ex) {
                    SQLRecorderLogger.ddlLogger.info(ex.toString());
                    if (tryTime > PhysicalBackfillUtils.MAX_RETRY) {
                        throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                    }
                    PhysicalBackfillUtils.checkInterrupted(ec, null);
                    tryTime++;
                }
            } while (!success);
        }
        msg = "already fallocate the ibd file:" + targetFileAndDir.getValue();
        SQLRecorderLogger.ddlLogger.info(msg);
    }

    private void copyCfgFile(final Pair<String, String> srcFileAndDir, final Pair<String, String> srcDbAndGroup,
                             final Pair<String, Integer> sourceHostIpAndPort,
                             final Pair<String, String> targetFileAndDir, final Pair<String, String> tarDbAndGroup,
                             final List<Pair<String, Integer>> targetHosts, BatchConsumer consumer) {

        String sourceStorageInstId = groupStorageInsts.get(srcDbAndGroup.getValue());
        String targetStorageInstId = groupStorageInsts.get(tarDbAndGroup.getValue());

        PolarxPhysicalBackfill.TransferFileDataOperator transferFileData = null;

        Pair<String, String> srcUserInfo = storageInstAndUserInfos.computeIfAbsent(sourceStorageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(sourceStorageInstId));
        Pair<String, String> tarUserInfo = storageInstAndUserInfos.computeIfAbsent(sourceStorageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(targetStorageInstId));

        long offset = 0l;
        do {
            boolean success = false;
            int tryTime = 0;
            do {
                try (XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(
                    srcDbAndGroup.getKey(),
                    sourceHostIpAndPort.getKey(), sourceHostIpAndPort.getValue(), srcUserInfo.getKey(),
                    srcUserInfo.getValue(), -1))) {

                    PolarxPhysicalBackfill.TransferFileDataOperator.Builder builder =
                        PolarxPhysicalBackfill.TransferFileDataOperator.newBuilder();

                    builder.setOperatorType(PolarxPhysicalBackfill.TransferFileDataOperator.Type.GET_DATA_FROM_SRC_IBD);
                    PolarxPhysicalBackfill.FileInfo.Builder fileInfoBuilder =
                        PolarxPhysicalBackfill.FileInfo.newBuilder();
                    fileInfoBuilder.setFileName(srcFileAndDir.getKey());
                    fileInfoBuilder.setTempFile(false);
                    fileInfoBuilder.setDirectory(srcFileAndDir.getValue());
                    fileInfoBuilder.setPartitionName("");
                    builder.setFileInfo(fileInfoBuilder.build());
                    builder.setBufferLen(batchSize);
                    builder.setOffset(offset);
                    transferFileData = conn.execReadBufferFromFile(builder);
                    success = true;
                } catch (Exception ex) {
                    if (tryTime >= PhysicalBackfillUtils.MAX_RETRY) {
                        throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                    }
                    tryTime++;
                }
            } while (!success);
            consumer.consume(tarDbAndGroup, targetFileAndDir, targetHosts, tarUserInfo,
                transferFileData);
            if (transferFileData.getBufferLen() < batchSize) {
                return;
            }
            offset += transferFileData.getBufferLen();
        } while (true);
    }
}
