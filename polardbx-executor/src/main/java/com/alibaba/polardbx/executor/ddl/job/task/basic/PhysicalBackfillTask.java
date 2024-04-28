package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.physicalbackfill.physicalBackfillLoader;
import com.alibaba.polardbx.gms.partition.PhysicalBackfillDetailInfoFieldJSON;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@Getter
@TaskName(name = "PhysicalBackfillTask")
public class PhysicalBackfillTask extends BaseDdlTask {

    private final String schemaName;
    private final String logicalTableName;
    private final Long backfillId;// use the taskId of CloneTableDataFileTask
    private final long batchSize;

    private final long parallelism;
    private final long minUpdateBatch;
    private final String physicalTableName;
    private final List<String> phyPartitionNames;
    private final Pair<String, String> sourceTargetGroup;
    private final Pair<String, String> sourceTargetDnId;
    private final boolean newPartitionDb;
    private final Map<String, Pair<String, String>> storageInstAndUserInfos;
    private final boolean waitLsn;

    //don't serialize those parameters
    private transient long lastUpdateTime = 0l;
    private transient Object lock = new Object();
    private transient volatile long curSpeedLimit;
    private transient PhysicalBackfillManager backfillManager;

    //todo broadcast table 1对N(新DN) N对M M=N*k k=副本数
    //type 不能是REFRESH_TOPOLOGY 需要是move table
    public PhysicalBackfillTask(String schemaName,
                                Long backfillId,
                                String logicalTableName,
                                String physicalTableName,
                                List<String> phyPartitionNames,
                                Pair<String, String> sourceTargetGroup,
                                Pair<String, String> sourceTargetDnId,
                                Map<String, Pair<String, String>> storageInstAndUserInfos,
                                long batchSize,
                                long parallelism,
                                long minUpdateBatch,
                                boolean waitLsn) {
        super(schemaName);
        this.schemaName = schemaName;
        this.backfillId = backfillId;
        this.logicalTableName = logicalTableName;
        this.physicalTableName = physicalTableName.toLowerCase();
        this.phyPartitionNames = phyPartitionNames;
        this.sourceTargetGroup = sourceTargetGroup;
        this.sourceTargetDnId = sourceTargetDnId;
        this.storageInstAndUserInfos = storageInstAndUserInfos;
        this.batchSize = batchSize;
        this.parallelism = Math.max(parallelism, 1);
        this.minUpdateBatch = minUpdateBatch;
        this.waitLsn = waitLsn;

        this.curSpeedLimit = OptimizerContext.getContext(schemaName).getParamManager()
            .getLong(ConnectionParams.PHYSICAL_BACKFILL_SPEED_LIMIT);
        this.newPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!newPartitionDb && sourceTargetGroup == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "missing source-target group mapping entry");
        }
        backfillManager = new PhysicalBackfillManager(schemaName);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        executeImpl(executionContext);
    }

    public void executeImpl(ExecutionContext ec) {
        physicalBackfillLoader loader = new physicalBackfillLoader(schemaName, logicalTableName);

        doExtract(ec, new BatchConsumer() {
            @Override
            public void consume(Pair<String, String> targetDbAndGroup,
                                Pair<String, String> targetFileAndDir,
                                List<Pair<String, Integer>> targetHosts,
                                Pair<String, String> userInfo,
                                PolarxPhysicalBackfill.TransferFileDataOperator transferFileData) {
                loader.applyBatch(targetDbAndGroup, targetFileAndDir, targetHosts, userInfo, transferFileData, ec);
            }
        });
    }

    protected void rollbackImpl(ExecutionContext ec) {
        //drop physical table before remove the ibd file
        // otherwise when tablespace is import, can't remove the .frm with drop table
        // and can't create the same table in the next round
        String dropPhyTable = "drop table if exists " + physicalTableName;

        ///!!!!!!DANGER!!!!
        // can't change variables via sql bypass CN
        // String disableBinlog = "SET SESSION sql_log_bin=0;
        HashMap<String, Object> variables = new HashMap<>();
        boolean ignore = false;
        try {
            DbGroupInfoRecord srcDbGroupInfoRecord =
                ScaleOutPlanUtil.getDbGroupInfoByGroupName(sourceTargetGroup.getKey());
            DbGroupInfoRecord tarDbGroupInfoRecord =
                ScaleOutPlanUtil.getDbGroupInfoByGroupName(sourceTargetGroup.getValue());

            PhysicalBackfillManager.BackfillBean physicalBackfillRecord =
                backfillManager.loadBackfillMeta(backfillId, schemaName, srcDbGroupInfoRecord.phyDbName.toLowerCase(),
                    physicalTableName,
                    GeneralUtil.isEmpty(phyPartitionNames) ? "" : phyPartitionNames.get(0));

            PhysicalBackfillDetailInfoFieldJSON detailInfoFieldJSON = physicalBackfillRecord.backfillObject.detailInfo;
            final String targetStorageId = sourceTargetDnId.getValue();
            Pair<String, String> userAndPasswd = storageInstAndUserInfos.get(targetStorageId);
            boolean healthyCheck =
                ec.getParamManager().getBoolean(ConnectionParams.PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK);
            for (Pair<String, Integer> targetHost : detailInfoFieldJSON.getTargetHostAndPorts()) {
                ignore = false;
                try (
                    XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(
                        tarDbGroupInfoRecord.phyDbName.toLowerCase(),
                        targetHost.getKey(), targetHost.getValue(), userAndPasswd.getKey(), userAndPasswd.getValue(),
                        -1))) {
                    try {
                        //disable sql_lon_bin
                        conn.setLastException(
                            new Exception("discard connection due to change SQL_LOG_BIN in this session"), true);
                        variables.put(PhysicalBackfillUtils.SQL_LOG_BIN, "OFF");
                        conn.setSessionVariables(variables);
                        SQLRecorderLogger.ddlLogger.info(
                            String.format(
                                "revert: begin to drop physical table before remove the ibd file %s, in host: %s, db:%s",
                                dropPhyTable,
                                targetHost, tarDbGroupInfoRecord.phyDbName.toLowerCase()));
                        conn.execQuery(dropPhyTable);
                        SQLRecorderLogger.ddlLogger.info(
                            String.format(
                                "revert: finish drop physical table before remove the ibd file %s, in host: %s, db:%s",
                                dropPhyTable,
                                targetHost, tarDbGroupInfoRecord.phyDbName.toLowerCase()));
                    } finally {
                        variables.clear();
                        //reset
                        conn.setSessionVariables(variables);
                    }
                } catch (Exception ex) {
                    if (ex != null && ex.toString() != null && ex.toString().indexOf("connect fail") != -1) {
                        List<Pair<String, Integer>> hostsIpAndPort =
                            PhysicalBackfillUtils.getMySQLServerNodeIpAndPorts(targetStorageId, healthyCheck);
                        Optional<Pair<String, Integer>> targetHostOpt =
                            hostsIpAndPort.stream().filter(o -> o.getKey().equalsIgnoreCase(targetHost.getKey())
                                && o.getValue().intValue() == targetHost.getValue().intValue()).findFirst();
                        if (!targetHostOpt.isPresent()) {
                            //maybe backup in other host
                            ignore = true;
                        }
                    }
                    throw ex;
                }
            }
        } catch (Exception ex) {
            SQLRecorderLogger.ddlLogger.info(
                "drop physical table error:" + ex == null ? "" : ex.toString() + " ignore=" + ignore);
            if (ignore) {
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex, "drop physical table error");
            }
        }

        PhysicalBackfillUtils.rollbackCopyIbd(backfillId, schemaName, logicalTableName, 2, ec);
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext ec) {
        if (waitLsn) {
            SQLRecorderLogger.ddlLogger.info("begin wait lsn when rollback PhysicalBackfillTask");
            Map<String, String> targetGroupAndStorageIdMap = new HashMap<>();
            targetGroupAndStorageIdMap.put(sourceTargetGroup.getValue(), sourceTargetDnId.getValue());
            PhysicalBackfillUtils.waitLsn(schemaName, targetGroupAndStorageIdMap, true, ec);
            SQLRecorderLogger.ddlLogger.info("finish wait lsn when rollback PhysicalBackfillTask");
        }
        rollbackImpl(ec);
    }

    @Override
    public String remark() {
        return "|physical backfill for table:" + physicalTableName + " from group:" + sourceTargetGroup.getKey()
            + " to " + sourceTargetGroup.getValue();
    }

    public void doExtract(ExecutionContext ec, BatchConsumer batchConsumer) {
        PhysicalBackfillUtils.checkInterrupted(ec, null);

        DbGroupInfoRecord srcDbGroupInfoRecord = ScaleOutPlanUtil.getDbGroupInfoByGroupName(sourceTargetGroup.getKey());
        DbGroupInfoRecord tarDbGroupInfoRecord =
            ScaleOutPlanUtil.getDbGroupInfoByGroupName(sourceTargetGroup.getValue());

        assert srcDbGroupInfoRecord != null;
        assert tarDbGroupInfoRecord != null;

        Pair<String, String> srcDbAndGroup =
            Pair.of(srcDbGroupInfoRecord.phyDbName.toLowerCase(), srcDbGroupInfoRecord.groupName);
        Pair<String, String> targetDbAndGroup =
            Pair.of(tarDbGroupInfoRecord.phyDbName.toLowerCase(), tarDbGroupInfoRecord.groupName);
        // in case restart this task and the GeneralUtil.isEmpty(phyPartNames)==false
        boolean hasNoPhyPart =
            GeneralUtil.isEmpty(phyPartitionNames) || phyPartitionNames.size() == 1 && StringUtils.isEmpty(
                phyPartitionNames.get(0));
        if (hasNoPhyPart && GeneralUtil.isEmpty(phyPartitionNames)) {
            phyPartitionNames.add("");
        }
        for (String phyPartName : phyPartitionNames) {
            foreachPhysicalFile(srcDbAndGroup, targetDbAndGroup, phyPartName, batchConsumer, ec);
        }

    }

    public void foreachPhysicalFile(final Pair<String, String> srcDbAndGroup,
                                    final Pair<String, String> targetDbAndGroup,
                                    final String phyPartName,
                                    final BatchConsumer consumer,
                                    final ExecutionContext ec) {

        //1 copy to target dn
        //2 delete temp ibd file

        String msg =
            "begin to backfill the idb file for table[" + srcDbAndGroup.getKey() + ":" + physicalTableName + "]";
        SQLRecorderLogger.ddlLogger.info(msg);

        Pair<String, String> srcUserInfo = storageInstAndUserInfos.get(sourceTargetDnId.getKey());
        List<Pair<Long, Long>> offsetAndSize = new ArrayList<>();

        final Pair<String, String> targetFileAndDir;

        PhysicalBackfillManager.BackfillBean initBean =
            backfillManager.loadBackfillMeta(backfillId, schemaName, srcDbAndGroup.getKey(), physicalTableName,
                phyPartName);
        Pair<String, String> srcFileAndDir = null;
        Pair<String, Integer> sourceHost = null;
        final Pair<String, String> tempFileAndDir;
        if (initBean.isEmpty() || initBean.isInit()) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                "the status of BackfillBean is empty or init");
        }

        if (initBean.isSuccess()) {
            return;
        }
        sourceHost = initBean.backfillObject.detailInfo.getSourceHostAndPort();
        srcFileAndDir = Pair.of(initBean.backfillObject.sourceFileName, initBean.backfillObject.sourceDirName);
        tempFileAndDir = srcFileAndDir;
        targetFileAndDir =
            Pair.of(initBean.backfillObject.targetFileName, initBean.backfillObject.targetDirName);
        //update the offsetAndSize
        PhysicalBackfillUtils.getTempIbdFileInfo(srcUserInfo, sourceHost, srcDbAndGroup, physicalTableName,
            phyPartName, srcFileAndDir, batchSize,
            true, offsetAndSize);

        BitSet bitSet;
        long[] bitSetPosMark = null;

        assert !initBean.isInit();

        PhysicalBackfillDetailInfoFieldJSON detailInfo = initBean.backfillObject.detailInfo;

        if (detailInfo != null) {
            bitSetPosMark = detailInfo.getBitSet();
        } else {
            detailInfo = new PhysicalBackfillDetailInfoFieldJSON();
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
                srcFileAndDir.getValue() + PhysicalBackfillUtils.TEMP_FILE_POSTFIX);
        } else {
            srcDir = PhysicalBackfillUtils.convertToCfgFileName(srcFileAndDir.getValue());
        }

        String tarFileName = targetFileAndDir.getKey();
        String tarDir = PhysicalBackfillUtils.convertToCfgFileName(targetFileAndDir.getValue());
        copyCfgFile(Pair.of(srcFileName, srcDir), srcDbAndGroup, sourceHostIpAndPort,
            Pair.of(tarFileName, tarDir), targetDbAndGroup, targetHost, consumer, ec);

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
        fallocateIbdFile(ec, targetFileAndDir, targetDbAndGroup, targetHost, physicalTableName, "", fileSize);

        // Use a bounded blocking queue to control the parallelism.
        BlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>((int) parallelism);

        AtomicInteger startPos = new AtomicInteger(0);

        for (int i = 0; i < parallelism; i++) {
            FutureTask<Void> task = new FutureTask<>(() -> {
                try {
                    doWork(srcDbAndGroup, targetDbAndGroup, tempFileAndDir,
                        targetFileAndDir, offsetAndSize, startPos, bitSet, batchSize, successBatch,
                        minUpdateBatch, phyPartName,
                        sourceHostIpAndPort, targetHost, consumer, ec, interrupted, excep);
                } finally {
                    // Poll in finally to prevent dead lock on putting blockingQueue.
                    blockingQueue.poll();
                }
                return null;
            });
            futures.add(task);
            BackFillThreadPool.getInstance()
                .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_BACKFILL_TASK);
            if (PhysicalBackfillUtils.miniBatchForeachThread * (i + 1) >= offsetAndSize.size()) {
                break;
            }
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
        PhysicalBackfillManager.BackfillBean bfb =
            backfillManager.loadBackfillMeta(backfillId, schemaName, srcDbAndGroup.getKey(), physicalTableName,
                phyPartName);
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

            backfillManager.updateBackfillObject(ImmutableList.of(bor));
            throw GeneralUtil.nestedException(excep.get());
        }
        bfb.backfillObject.detailInfo.setBitSet(null);
        bfb.backfillObject.detailInfo.setMsg("");
        bor.setStatus((int) PhysicalBackfillManager.BackfillStatus.SUCCESS.getValue());
        bor.setDetailInfo(PhysicalBackfillDetailInfoFieldJSON.toJson(bfb.backfillObject.detailInfo));
        bor.setSuccessBatchCount(offsetAndSize.size());

        Pair<String, Integer> ipPortPair = bfb.backfillObject.detailInfo.getSourceHostAndPort();

        PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, srcDbAndGroup.getValue(), srcDbAndGroup.getKey(),
            ipPortPair.getKey(), ipPortPair.getValue(),
            PhysicalBackfillUtils.convertToCfgFileName(tempFileAndDir.getValue()), false, ec);

        PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, srcDbAndGroup.getValue(), srcDbAndGroup.getKey(),
            ipPortPair.getKey(), ipPortPair.getValue(), tempFileAndDir.getValue(), false, ec);
        // After all physical table finished
        backfillManager.updateBackfillObject(ImmutableList.of(bor));

        msg = "already backfill the idb file for table[" + srcDbAndGroup.getKey() + ":" + physicalTableName + "]"
            + phyPartName;
        SQLRecorderLogger.ddlLogger.info(msg);
    }

    private void doWork(final Pair<String, String> srcDbAndGroup,
                        final Pair<String, String> targetDbAndGroup,
                        final Pair<String, String> srcFileAndDir,
                        final Pair<String, String> targetFileAndDir,
                        final List<Pair<Long, Long>> totalOffsetAndSize,
                        final AtomicInteger startPos,
                        final BitSet bitSet,
                        long batchSize,
                        final AtomicInteger successBatch,
                        final long minUpdateBatch,
                        final String phyPartName,
                        final Pair<String, Integer> sourceHost,
                        final List<Pair<String, Integer>> targetHost,
                        final BatchConsumer consumer,
                        final ExecutionContext ec,
                        final AtomicReference<Boolean> interrupted,
                        final AtomicReference<Exception> excep) {

        do {
            int pos = startPos.getAndAdd(PhysicalBackfillUtils.miniBatchForeachThread);
            for (int i = pos; i < pos + PhysicalBackfillUtils.miniBatchForeachThread; i++) {
                if (i >= totalOffsetAndSize.size()) {
                    return;
                }
                Pair<Long, Long> offsetAndSize = totalOffsetAndSize.get(i);

                int index = (int) (offsetAndSize.getKey() / batchSize);
                if (!bitSet.get(index)) {
                    if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()
                        || interrupted.get()) {
                        long jobId = ec.getDdlJobId();
                        excep.set(new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "The job '" + jobId + "' has been cancelled"));
                        interrupted.set(true);
                        return;
                    }

                    PolarxPhysicalBackfill.TransferFileDataOperator transferFileData = null;

                    Pair<String, String> srcUserInfo = storageInstAndUserInfos.get(sourceTargetDnId.getKey());
                    Pair<String, String> tarUserInfo = storageInstAndUserInfos.get(sourceTargetDnId.getValue());
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
                            XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(
                                srcDbAndGroup.getKey(),
                                sourceHost.getKey(), sourceHost.getValue(), srcUserInfo.getKey(),
                                srcUserInfo.getValue(),
                                -1))) {
                            PolarxPhysicalBackfill.TransferFileDataOperator.Builder builder =
                                PolarxPhysicalBackfill.TransferFileDataOperator.newBuilder();

                            builder.setOperatorType(
                                PolarxPhysicalBackfill.TransferFileDataOperator.Type.GET_DATA_FROM_SRC_IBD);
                            PolarxPhysicalBackfill.FileInfo.Builder fileInfoBuilder =
                                PolarxPhysicalBackfill.FileInfo.newBuilder();
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

                            PhysicalBackfillManager.BackfillBean bfb =
                                backfillManager.loadBackfillMeta(backfillId, schemaName, srcDbAndGroup.getKey(),
                                    physicalTableName, phyPartName);

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
                            bor.setEndTime(
                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()));
                            bor.setStatus((int) PhysicalBackfillManager.BackfillStatus.RUNNING.getValue());
                            bor.setDetailInfo(PhysicalBackfillDetailInfoFieldJSON.toJson(detailInfo));

                            backfillManager.updateBackfillObject(ImmutableList.of(bor));

                            successBatch.set(0);

                            double speed =
                                (curSuccessBatch * batchSize) * 1000.0 / Math.max(1, curTime - lastUpdateTime) / 1024;

                            //todo calc the speed by 1000 batch / time
                            String msg =
                                "already write " + curSuccessBatch + " batch successfully for "
                                    + srcFileAndDir.getValue()
                                    + " speed:" + df.format(speed) + "KB/s the maximum speed limit:"
                                    + df.format(PhysicalBackfillUtils.getRateLimiter().getRate() / 1024) + "KB/s";
                            SQLRecorderLogger.ddlLogger.info(msg);
                            lastUpdateTime = System.currentTimeMillis();
                        }
                    }
                }
            }
        } while (true);
    }

    private void fallocateIbdFile(final ExecutionContext ec, final Pair<String, String> targetFileAndDir,
                                  final Pair<String, String> tarDbAndGroup,
                                  final List<Pair<String, Integer>> targetHosts, String physicalTableName,
                                  String phyPartitionName, long fileSize) {
        String msg = "begin to fallocate ibd file:" + targetFileAndDir.getValue();
        SQLRecorderLogger.ddlLogger.info(msg);

        Pair<String, String> userInfo = storageInstAndUserInfos.get(sourceTargetDnId.getValue());

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
                             final List<Pair<String, Integer>> targetHosts, BatchConsumer consumer,
                             ExecutionContext ec) {

        //delete first before copy,because do not have backfillMeta for cfg file

        for (Pair<String, Integer> pair : GeneralUtil.emptyIfNull(
            targetHosts)) {
            PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, tarDbAndGroup.getValue(), tarDbAndGroup.getKey(),
                pair.getKey(), pair.getValue(), targetFileAndDir.getValue(), true, ec);
        }
        PolarxPhysicalBackfill.TransferFileDataOperator transferFileData = null;

        Pair<String, String> srcUserInfo = storageInstAndUserInfos.get(sourceTargetDnId.getKey());
        Pair<String, String> tarUserInfo = storageInstAndUserInfos.get(sourceTargetDnId.getValue());

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
