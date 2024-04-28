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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillReporter;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.gms.partition.PhysicalBackfillDetailInfoFieldJSON;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.common.TddlConstants.LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN;

@Getter
@TaskName(name = "CloneTableDataFileTask")
public class CloneTableDataFileTask extends BaseDdlTask {

    final Pair<String, String> srcDbAndGroup;
    final Pair<String, String> tarDbAndGroup;
    final String logicalTableName;
    final String phyTableName;
    final List<String> phyPartNames;
    final Pair<String, Integer> sourceHostIpAndPort;
    final List<Pair<String, Integer>> targetHostsIpAndPort;
    final String sourceStorageInstId;
    final Long batchSize;
    //don't serialize this parameter
    private transient Map<String, Pair<String, String>> storageInstAndUserInfos = new ConcurrentHashMap<>();

    @JSONCreator
    public CloneTableDataFileTask(String schemaName, String logicalTableName, Pair<String, String> srcDbAndGroup,
                                  Pair<String, String> tarDbAndGroup, String phyTableName, List<String> phyPartNames,
                                  String sourceStorageInstId, Pair<String, Integer> sourceHostIpAndPort,
                                  List<Pair<String, Integer>> targetHostsIpAndPort, Long batchSize) {
        super(schemaName);
        this.srcDbAndGroup = srcDbAndGroup;
        this.tarDbAndGroup = tarDbAndGroup;
        this.logicalTableName = logicalTableName;
        this.phyTableName = phyTableName.toLowerCase();
        this.phyPartNames = phyPartNames;
        this.sourceStorageInstId = sourceStorageInstId;
        this.sourceHostIpAndPort = sourceHostIpAndPort;
        this.targetHostsIpAndPort = targetHostsIpAndPort;
        this.batchSize = batchSize;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        executeImpl(executionContext);
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext ec) {
        rollbackImpl(ec);
    }

    public void executeImpl(ExecutionContext ec) {
        PhysicalBackfillManager backfillManager = new PhysicalBackfillManager(schemaName);
        PhysicalBackfillReporter reporter = new PhysicalBackfillReporter(backfillManager);

        // in case restart this task and the GeneralUtil.isEmpty(phyPartNames)==false
        boolean hasNoPhyPart =
            GeneralUtil.isEmpty(phyPartNames) || phyPartNames.size() == 1 && StringUtils.isEmpty(phyPartNames.get(0));
        if (hasNoPhyPart && GeneralUtil.isEmpty(phyPartNames)) {
            phyPartNames.add("");
        }
        Pair<String, String> userInfo = storageInstAndUserInfos.computeIfAbsent(sourceStorageInstId,
            key -> PhysicalBackfillUtils.getUserPasswd(sourceStorageInstId));

        Map<String, Pair<String, String>> srcFileAndDirs =
            PhysicalBackfillUtils.getSourceTableInfo(userInfo, srcDbAndGroup.getKey(), phyTableName, phyPartNames,
                hasNoPhyPart, sourceHostIpAndPort);

        DbGroupInfoRecord tarDbGroupInfoRecord = ScaleOutPlanUtil.getDbGroupInfoByGroupName(tarDbAndGroup.getValue());

        initBackfillMeta(reporter, phyPartNames, srcFileAndDirs, tarDbGroupInfoRecord, ec);
        cloneInnodbDataFile(hasNoPhyPart, srcFileAndDirs, userInfo, ec);
        updateBackfillStatus(reporter, srcFileAndDirs, userInfo, ec);
    }

    public void rollbackImpl(ExecutionContext ec) {
        PhysicalBackfillUtils.rollbackCopyIbd(getTaskId(), schemaName, logicalTableName, 1, ec);
    }

    @Override
    public String remark() {
        return "|clone data for table:" + phyTableName + " in db:" + srcDbAndGroup.getKey() + " host:"
            + sourceHostIpAndPort;
    }

    private void initBackfillMeta(PhysicalBackfillReporter reporter, List<String> phyPartNames,
                                  Map<String, Pair<String, String>> srcFileAndDirs,
                                  DbGroupInfoRecord tarDbGroupInfoRecord, ExecutionContext ec) {
        //use this taskId as backfill id, and pass this id to backfill task as input
        PhysicalBackfillManager.BackfillBean initBean =
            reporter.loadBackfillMeta(getTaskId(), schemaName, srcDbAndGroup.getKey(), phyTableName,
                phyPartNames.get(0));
        if (!initBean.isEmpty()) {
            for (String phyPart : phyPartNames) {
                PhysicalBackfillManager.BackfillBean backfillBean =
                    reporter.loadBackfillMeta(getTaskId(), schemaName, srcDbAndGroup.getKey(), phyTableName, phyPart);
                assert backfillBean.isInit();
                PhysicalBackfillManager.BackfillObjectBean bean = backfillBean.backfillObject;
                try {
                    PhysicalBackfillDetailInfoFieldJSON detailInfoFieldJSON = bean.detailInfo;
                    PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, bean.sourceGroupName, bean.physicalDb,
                        detailInfoFieldJSON.getSourceHostAndPort().getKey(),
                        detailInfoFieldJSON.getSourceHostAndPort().getValue(),
                        PhysicalBackfillUtils.convertToCfgFileName(bean.sourceDirName), true, ec);
                    PhysicalBackfillUtils.deleteInnodbDataFile(schemaName, bean.sourceGroupName, bean.physicalDb,
                        detailInfoFieldJSON.getSourceHostAndPort().getKey(),
                        detailInfoFieldJSON.getSourceHostAndPort().getValue(), bean.sourceDirName, true, ec);
                } catch (Exception ex) {
                    //ignore
                    try {
                        SQLRecorderLogger.ddlLogger.info(ex.toString());
                    } catch (Exception e) {

                    }
                }

                reporter.getBackfillManager().deleteById(bean.id);
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
            partTargetDir =
                PhysicalBackfillUtils.IDB_DIR_PREFIX + tarDbGroupInfoRecord.phyDbName.toLowerCase() + partTargetDir;

            Pair<String, String> partTargetFileAndDir = new Pair<>(partTargetFile, partTargetDir);

            reporter.getBackfillManager()
                .insertBackfillMeta(schemaName, logicalTableName, getTaskId(), srcDbAndGroup.getKey(), phyTableName,
                    entry.getKey(), srcDbAndGroup.getValue(), tarDbAndGroup.getValue(), partTempFileAndDir,
                    partTargetFileAndDir, 0, batchSize, 0, 0, sourceHostIpAndPort, targetHostsIpAndPort);

        }
    }

    private void cloneInnodbDataFile(boolean hasNoPhyPart, Map<String, Pair<String, String>> srcFileAndDirs,
                                     Pair<String, String> userInfo, ExecutionContext ec) {

        String msg = "begin to clone the files for table:" + phyTableName;
        SQLRecorderLogger.ddlLogger.info(msg);
        XConnection conn = null;

        boolean success = false;
        int tryTime = 1;
        StringBuilder copyFileInfo = null;
        AtomicReference<Boolean> finished = new AtomicReference<>(false);
        do {
            try {
                copyFileInfo = new StringBuilder();
                conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(srcDbAndGroup.getKey(),
                    sourceHostIpAndPort.getKey(), sourceHostIpAndPort.getValue(), userInfo.getKey(),
                    userInfo.getValue(), -1));
                conn.setNetworkTimeoutNanos(LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN * 1000000L);
                conn.execQuery(String.format(PhysicalBackfillUtils.FLUSH_TABLE_SQL_TEMPLATE, phyTableName));
                PolarxPhysicalBackfill.FileManageOperator.Builder builder =
                    PolarxPhysicalBackfill.FileManageOperator.newBuilder();

                PolarxPhysicalBackfill.TableInfo.Builder tableInfoBuilder =
                    PolarxPhysicalBackfill.TableInfo.newBuilder();
                tableInfoBuilder.setTableSchema(srcDbAndGroup.getKey());
                tableInfoBuilder.setTableName(phyTableName);
                tableInfoBuilder.setPartitioned(hasNoPhyPart);
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
                            directory = PhysicalBackfillUtils.convertToCfgFileName(directory);
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
                            SQLRecorderLogger.ddlLogger.info("exeCloneFile session was terminated, sessionId");
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
                    sourceHostIpAndPort.getKey(), sourceHostIpAndPort.getValue().toString(), srcDbAndGroup.getKey());
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

    private void updateBackfillStatus(PhysicalBackfillReporter reporter,
                                      Map<String, Pair<String, String>> srcFileAndDirs, Pair<String, String> userInfo,
                                      ExecutionContext ec) {
        for (Map.Entry<String, Pair<String, String>> entry : srcFileAndDirs.entrySet()) {

            List<Pair<Long, Long>> offsetAndSize = new ArrayList<>();
            PhysicalBackfillUtils.getTempIbdFileInfo(userInfo, sourceHostIpAndPort, srcDbAndGroup, phyTableName,
                entry.getKey(), entry.getValue(), batchSize, false, offsetAndSize);

            PhysicalBackfillManager.BackfillBean backfillBean =
                reporter.loadBackfillMeta(getTaskId(), schemaName, srcDbAndGroup.getKey(), phyTableName,
                    entry.getKey());
            assert backfillBean.isInit();

            reporter.getBackfillManager()
                .updateStatusAndTotalBatch(backfillBean.backfillObject.id, offsetAndSize.size());
        }
    }

}
