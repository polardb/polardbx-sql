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

package com.alibaba.polardbx.executor.ddl.newengine.cross;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.PhyDdlExecutionRecord;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.BACKTICK;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.EMPTY_CONTENT;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ERROR_CANT_DROP_KEY;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ERROR_DUPLICATE_KEY;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ERROR_TABLE_EXISTS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ERROR_UNKNOWN_TABLE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.SEMICOLON;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.SQLSTATE_TABLE_EXISTS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.SQLSTATE_UNKNOWN_TABLE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.SQLSTATE_VIOLATION;

public class GenericPhyObjectRecorder {

    protected static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    protected final ExecutionContext executionContext;
    protected final DdlContext ddlContext;

    protected final PhyDdlTableOperation physicalDdlPlan;

    protected final String schemaName;
    protected final String groupName;
    protected final String phyTableName;

    protected final PhyDdlExecutionRecord phyDdlExecutionRecord;

    public GenericPhyObjectRecorder(RelNode physicalPlan, ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.ddlContext = executionContext.getDdlContext();

        String objectSchema;
        if (physicalPlan instanceof PhyDdlTableOperation) {
            this.physicalDdlPlan = (PhyDdlTableOperation) physicalPlan;
            objectSchema = physicalDdlPlan.getSchemaName();
            Pair<String, String> phyTablePair = DdlHelper.genPhyTablePair(physicalDdlPlan, ddlContext);
            groupName = phyTablePair.getKey();
            phyTableName = phyTablePair.getValue();
        } else {
            this.physicalDdlPlan = null;
            objectSchema = null;
            groupName = null;
            phyTableName = null;
        }

        this.schemaName = TStringUtil.isEmpty(objectSchema) ? executionContext.getSchemaName() : objectSchema;

        this.phyDdlExecutionRecord = executionContext.getPhyDdlExecutionRecord();
    }

    public boolean checkIfDone() {
        if (physicalDdlPlan == null) {
            return false;
        }

        if (!executionContext.needToRenamePhyTables()) {
            return true;
        }

        if (!ExecUtils.hasLeadership(schemaName)) {
            // The node doesn't have leadership any longer, so let's terminate current job.
            String nodeInfo = TddlNode.getNodeInfo();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Loss of leadership on " + nodeInfo + ". Current job will be taken over by new leader later");
        }

        // If the job has been cancelled via user command or kill handler, then
        // we should throw an exception to terminate the DDL execution.
        if (ddlContext.isInterrupted()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + ddlContext.getJobId() + "' has been interrupted");
        }

        return checkIfPhyObjectDone();
    }

    public void recordDone() {
        if (physicalDdlPlan == null) {
            return;
        }

        if (isCurrentPlanSuccessful()) {
            if (ddlContext.getState() == DdlState.ROLLBACK_RUNNING) {
                recordObjectRollback();
            } else {
                recordObjectNormal();
            }
        }
    }

    protected boolean isCurrentPlanSuccessful() {
        boolean successful = true;

        List<ExecutionContext.ErrorMessage> errorMessages =
            (List<ExecutionContext.ErrorMessage>) executionContext.getExtraDatas().get(ExecutionContext.FailedMessage);

        if (GeneralUtil.isNotEmpty(errorMessages)) {
            // Copy a new list to avoid conflict since original list may be updated concurrently.
            List<ExecutionContext.ErrorMessage> currentErrorMessages = new ArrayList<>(errorMessages);

            String tableName = phyTableName.replaceAll(BACKTICK, EMPTY_CONTENT);

            for (ExecutionContext.ErrorMessage errorMessage : currentErrorMessages) {
                if (errorMessage != null && errorMessage.getMessage() != null) {
                    String pureErrorMessage = errorMessage.getMessage().replaceAll(BACKTICK, EMPTY_CONTENT);
                    if (TStringUtil.equalsIgnoreCase(errorMessage.getGroupName(), groupName) &&
                        TStringUtil.containsIgnoreCase(pureErrorMessage, tableName)) {
                        // Check if we can ignore the error.
                        successful = checkIfIgnoreSqlStateAndErrorCode(null, errorMessage.getCode());
                        if (successful) {
                            // Record the error message for final determination.
                            phyDdlExecutionRecord.addErrorIgnored(errorMessage);
                        } else {
                            // Check if the physical object is actually done.
                            successful = checkIfPhyObjectDoneByHashcode();
                            if (successful) {
                                // Record the error message for final determination.
                                phyDdlExecutionRecord.addErrorIgnored(errorMessage);
                            }
                        }
                    }
                }
            }
        }

        return successful;
    }

    protected void recordObjectNormal() {
        String phyObjectInfo = genPhyObjectInfo();
        recordObjectNormal(phyObjectInfo, true);
    }

    protected void recordObjectNormal(String phyObjectInfo, boolean afterPhyDdl) {
        addPhyObjectDone(phyObjectInfo, afterPhyDdl);

        DdlJobManagerUtils.appendPhyTableDone(phyDdlExecutionRecord, phyObjectInfo, afterPhyDdl);

        printDebugInfo("GenericPhyObjectRecorder.recordObjectNormal() - " + afterPhyDdl,
            phyDdlExecutionRecord, phyObjectInfo);
    }

    protected void recordObjectRollback() {
        resetPhyObjectsDone(genPhyObjectInfo(), true);
    }

    protected void resetPhyObjectsDone(String phyObjectInfo, boolean afterPhyDdl) {
        synchronized (GenericPhyObjectRecorder.class) {
            removePhyObjectDone(phyObjectInfo, afterPhyDdl);

            // Build new object done list.
            StringBuilder buf = new StringBuilder();
            phyDdlExecutionRecord.getPhyObjectsDone().stream()
                .forEach(phyObjectDone -> buf.append(SEMICOLON).append(phyObjectDone));

            String newPhyObjectsDone = buf.length() > 0 ? buf.deleteCharAt(0).toString() : "";

            DdlJobManagerUtils.resetPhyTablesDone(phyDdlExecutionRecord, newPhyObjectsDone);

            printDebugInfo("GenericPhyObjectRecorder.recordObjectRollback() - " + afterPhyDdl,
                phyDdlExecutionRecord, phyObjectInfo);
        }
    }

    public boolean checkIfIgnoreException(Throwable t) {
        if (physicalDdlPlan == null) {
            return false;
        }

        boolean exceptionIgnored = false;

        if (t != null && ddlContext.getState() != DdlState.ROLLBACK_RUNNING) {
            if (t instanceof SQLException) {
                SQLException e = (SQLException) t;
                exceptionIgnored = checkIfIgnoreSqlStateAndErrorCode(e.getSQLState(), e.getErrorCode());
            } else if (t instanceof TddlRuntimeException) {
                TddlRuntimeException e = (TddlRuntimeException) t;
                exceptionIgnored = checkIfIgnoreSqlStateAndErrorCode(e.getSQLState(), e.getErrorCode());
            }
        }

        if (exceptionIgnored) {
            recordDone();
        }

        return exceptionIgnored;
    }

    // A subclass may need to override the following methods.

    protected boolean checkIfPhyObjectDone() {
        String phyObjectInfo = genPhyObjectInfo();
        return phyDdlExecutionRecord.getPhyObjectsDone().contains(phyObjectInfo);
    }

    protected boolean checkIfPhyObjectDoneByHashcode() {
        return false;
    }

    protected void addPhyObjectDone(String phyObjectInfo, boolean afterPhyDdl) {
        phyDdlExecutionRecord.addPhyObjectDone(phyObjectInfo);
        if (afterPhyDdl) {
            phyDdlExecutionRecord.increasePhyObjsDone();
        }
    }

    protected void removePhyObjectDone(String phyObjectInfo, boolean afterPhyDdl) {
        phyDdlExecutionRecord.removePhyObjectDone(phyObjectInfo);
        if (afterPhyDdl) {
            phyDdlExecutionRecord.decreasePhyObjsDone();
        }
    }

    protected String genPhyObjectInfo() {
        return DdlHelper.genPhyTableInfo(groupName, phyTableName);
    }

    protected boolean checkIfIgnoreSqlStateAndErrorCode(String sqlState, int errorCode) {
        boolean errorIgnored = false;

        if (ddlContext.getState() == DdlState.ROLLBACK_RUNNING) {
            // Don't support idempotent rollback for now.
            switch (ddlContext.getDdlType()) {
            case CREATE_TABLE:
            case CREATE_GLOBAL_INDEX:
                break;
            case CREATE_INDEX:
                break;
            case DROP_TABLE:
            case DROP_GLOBAL_INDEX:
            case DROP_INDEX:
                // Don't support rolling back the DROP operations.
            default:
                // Don't check for other types of DDL operations.
                break;
            }
        } else {
            // If the sql state and/or error code match the following for
            // various DDL operations, it means that the object has been
            // done during previous normal performing, but hasn't been
            // recorded yet, so we should ignore such exception and record
            // the object done.
            switch (ddlContext.getDdlType()) {
            case CREATE_TABLE:
            case CREATE_GLOBAL_INDEX:
            case RENAME_TABLE:
            case RENAME_GLOBAL_INDEX:
                // Note that RENAME TABLE will check if the target name
                // exists, so if a RENAME TABLE operation has been done,
                // but redo it again, then the same sql state and/or error
                // code will be returned as CREATE TABLE.
                errorIgnored =
                    (TStringUtil.isEmpty(sqlState) || TStringUtil.equalsIgnoreCase(sqlState, SQLSTATE_TABLE_EXISTS))
                        && errorCode == ERROR_TABLE_EXISTS;
                break;
            case CREATE_INDEX:
                errorIgnored =
                    (TStringUtil.isEmpty(sqlState) || TStringUtil.equalsIgnoreCase(sqlState, SQLSTATE_VIOLATION))
                        && errorCode == ERROR_DUPLICATE_KEY;
                break;
            case DROP_TABLE:
            case DROP_GLOBAL_INDEX:
                errorIgnored =
                    (TStringUtil.isEmpty(sqlState) || TStringUtil.equalsIgnoreCase(sqlState, SQLSTATE_UNKNOWN_TABLE))
                        && errorCode == ERROR_UNKNOWN_TABLE;
                break;
            case DROP_INDEX:
                errorIgnored =
                    (TStringUtil.isEmpty(sqlState) || TStringUtil.equalsIgnoreCase(sqlState, SQLSTATE_VIOLATION))
                        && errorCode == ERROR_CANT_DROP_KEY;
                break;
            default:
                // Don't check for other types of DDL operations.
                break;
            }
        }

        return errorIgnored;
    }

    protected void printDebugInfo(String location, PhyDdlExecutionRecord phyDdlExecutionRecord, String phyObjectInfo) {
        if (executionContext.getParamManager().getBoolean(ConnectionParams.DDL_SHARD_CHANGE_DEBUG)) {
            StringBuilder buf = new StringBuilder();
            buf.append("*** DDL DEBUG ***\n\n");
            buf.append(location).append("\n\n");
            buf.append(phyDdlExecutionRecord.toString()).append("\n");
            buf.append("Current Physical Object Info:\n").append(phyObjectInfo).append("\n\n");
            LOGGER.info(buf.toString());
        }
    }

}
