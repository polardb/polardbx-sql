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
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.gms.metadb.misc.ImportTableSpaceInfoStatAccessor;
import com.alibaba.polardbx.gms.metadb.misc.ImportTableSpaceInfoStatRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.SqlIdentifierUtil;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DataSize;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.common.TddlConstants.LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.DN_CPU;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.DN_IO;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.DN_SYSTEM_LOCK;

@Getter
@TaskName(name = "ImportTableSpaceDdlTask")
public class ImportTableSpaceDdlTask extends BaseDdlTask implements RemoteExecutableDdlTask {

    private String logicalTableName;
    private String phyDbName;
    private String phyTableName;
    private Pair<String, Integer> targetHost;//leader、follower or leaner
    private Pair<String, String> userAndPasswd;
    private long dataSize;
    private long ioAdvise;

    @JSONCreator
    public ImportTableSpaceDdlTask(String schemaName, String logicalTableName, String phyDbName, String phyTableName,
                                   Pair<String, Integer> targetHost, Pair<String, String> userAndPasswd,
                                   String targetDnId,
                                   long dataSize,
                                   long ioAdvise) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.phyDbName = phyDbName.toLowerCase();
        this.phyTableName = phyTableName.toLowerCase();
        this.targetHost = targetHost;
        this.userAndPasswd = userAndPasswd;
        this.dataSize = dataSize;
        this.ioAdvise = ioAdvise;
        setResourceAcquired(buildResourceRequired(targetDnId, phyDbName + "//" + phyTableName, dataSize));
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        executeImpl(executionContext);
    }

    DdlEngineResources buildResourceRequired(String targetDnId, String phyDbTableName, long dataSize) {
        DdlEngineResources resourceRequired = new DdlEngineResources();
        String owner = "ImportTableSpace:" + phyDbTableName;
        long importTableSpaceIo = 40L;
        if (ioAdvise >= 10L && ioAdvise <= 100L) {
            importTableSpaceIo = ioAdvise;
        }
        String fullDnResourceName = DdlEngineResources.concateDnResourceName(targetHost, targetDnId);
        resourceRequired.requestForce(fullDnResourceName + DN_IO, importTableSpaceIo, owner);
        resourceRequired.requestForce(fullDnResourceName + DN_CPU, 25L, owner);
        resourceRequired.requestForce(fullDnResourceName + DN_SYSTEM_LOCK, 40L, owner);
        return resourceRequired;
    }

    @Override
    public DdlEngineResources getDdlEngineResources() {
        return this.resourceAcquired;
    }

    public void executeImpl(ExecutionContext executionContext) {
        ///!!!!!!DANGER!!!!
        // can't change variables via sql bypass CN
        // String disableBinlog = "SET SESSION sql_log_bin=0;
        HashMap<String, Object> variables = new HashMap<>();
        //disable sql_lon_bin
        variables.put(PhysicalBackfillUtils.SQL_LOG_BIN, "OFF");
        String importTableSpace =
            String.format("alter table %s import tablespace", SqlIdentifierUtil.escapeIdentifierString(phyTableName));
        try (
            XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(phyDbName,
                targetHost.getKey(), targetHost.getValue(), userAndPasswd.getKey(), userAndPasswd.getValue(), -1))) {
            try {
                conn.setLastException(new Exception("discard connection due to change SQL_LOG_BIN in this session"),
                    true);
                conn.setNetworkTimeoutNanos(LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN * 1000000L);
                //disable sql_lon_bin
                conn.setSessionVariables(variables);
                SQLRecorderLogger.ddlLogger.info(
                    String.format("begin to execute import tablespace command %s, in host: %s, db:%s", importTableSpace,
                        targetHost, phyDbName));
                long startTime = System.currentTimeMillis();
                conn.execQuery(importTableSpace);
                addImportTableSpaceStat(startTime);
                SQLRecorderLogger.ddlLogger.info(
                    String.format("finish execute import tablespace command %s, in host: %s, db:%s", importTableSpace,
                        targetHost, phyDbName));
            } finally {
                variables.clear();
                //reset
                conn.setSessionVariables(variables);
            }

        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex, "import tablespace error");
        }
    }

    private boolean tableSpaceExistError(String errMsg) {
        if (StringUtils.isEmpty(errMsg)) {
            return false;
        }
        String pattern = "tablespace.*exists";
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(errMsg.toLowerCase());
        if (matcher.find()) {
            return true;
        } else {
            return false;
        }
    }

    public void rollbackImpl(ExecutionContext executionContext) {
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext ec) {
        rollbackImpl(ec);
    }

    @Override
    public String remark() {
        return "|alter table " + phyTableName + " import tablespace, phyDb:" + phyDbName + " host:" + targetHost
            + " dataSize: " + DataSize.succinctBytes(dataSize);
    }

    public List<String> explainInfo() {
        String importTableSpace = "alter table " + phyTableName + " import tablespace";
        List<String> command = new ArrayList<>(1);
        command.add(importTableSpace);
        return command;
    }

    private void addImportTableSpaceStat(long startTime) {
        try (Connection metadb = MetaDbUtil.getConnection()) {
            ImportTableSpaceInfoStatRecord infoStatRecord = new ImportTableSpaceInfoStatRecord();
            infoStatRecord.setTaskId(taskId);
            infoStatRecord.setTableSchema(schemaName);
            infoStatRecord.setTableName(logicalTableName);
            infoStatRecord.setPhysicalDb(phyDbName);
            infoStatRecord.setPhysicalTable(phyTableName);
            infoStatRecord.setDataSize(dataSize);
            infoStatRecord.setStartTime(startTime);
            infoStatRecord.setEndTime(System.currentTimeMillis());
            ImportTableSpaceInfoStatAccessor importTableSpaceInfoStatAccessor = new ImportTableSpaceInfoStatAccessor();
            importTableSpaceInfoStatAccessor.setConnection(metadb);
            importTableSpaceInfoStatAccessor.insert(ImmutableList.of(infoStatRecord));
            metadb.commit();
        } catch (Exception ex) {
            SQLRecorderLogger.ddlLogger.info(
                String.format("fail to insert into IMPORT_TABLESPACE_INFO_STAT due to %s ", ex.toString()));
        }
    }
}
