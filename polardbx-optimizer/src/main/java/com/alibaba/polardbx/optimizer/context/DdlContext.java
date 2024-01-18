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

package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.lease.LeaseRecord;
import com.alibaba.polardbx.optimizer.statis.SQLRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DDL-relevant runtime data & state
 */
public class DdlContext {

    private long jobId;
    private DdlType ddlType;
    private String schemaName;
    private String objectName;
    private String traceId;
    private Set<String> resources = new HashSet<>();
    private String ddlStmt;
    private transient boolean isSubJob = false;
    private transient long parentJobId = 0;
    private transient long parentTaskId = 0;
    private transient boolean forRollback = false;
    private transient boolean isFkRepartition = false;
    private DdlContext parentDdlContext = null;
    private boolean ignoreCdcGsiMark = false;
    private boolean isGSI = false;
    private DdlState pausedPolicy = DdlState.RUNNING;
    private DdlState rollbackPausedPolicy = DdlState.ROLLBACK_RUNNING;

    private ConcurrentHashMap<Long, AtomicBoolean> physicalDdlInjectionFlag = new ConcurrentHashMap<>();

    private transient AtomicReference<Boolean> clientConnectionReset = new AtomicReference<>(false);
    /**
     * Get current DDL state.
     */
    private transient AtomicReference<DdlState> state = new AtomicReference<>();
    /**
     * whether current execution is interrupted
     */
    private transient AtomicReference<Boolean> interrupted = new AtomicReference<>(false);

    private transient AtomicReference<Boolean> rollbackToReady = new AtomicReference<>(false);

    private transient AtomicReference<LeaseRecord> jobLease = new AtomicReference<>();

    /**
     * from ExecutionContext.enableTrace
     */
    private boolean enableTrace;

    private String responseNode;

    private boolean asyncMode = false;

    private boolean usingWarning = false;

    private Map<String, Object> dataPassed = new HashMap<>();
    private Map<String, Object> serverVariables = new HashMap<>();
    private Map<String, Object> userDefVariables = new HashMap<>();
    private Map<String, Object> extraServerVariables = new HashMap<>();
    private Map<String, Object> extraCmds = new HashMap<>();
    private String encoding;
    private String timeZone;

    private Boolean explain;

    private String errorMessage;

    private String sqlMode;

    // rewrite origin sql for different fk naming behaviours in 5.7 & 8.0
    private String foreignKeyOriginalSql;

    public static DdlContext create(String schemaName, String objectName, DdlType ddlType,
                                    ExecutionContext executionContext) {
        DdlContext ddlContext = new DdlContext();

        ddlContext.setDdlType(ddlType);
        ddlContext.setSchemaName(schemaName);
        ddlContext.setObjectName(objectName);
        ddlContext.setTraceId(executionContext.getTraceId());
        ddlContext.setEnableTrace(executionContext.isEnableTrace());

        String ddlStmt;
        MultiDdlContext multiDdlContext = executionContext.getMultiDdlContext();
        int numOfPlans = multiDdlContext.getNumOfPlans();
        if (numOfPlans > 1) {
            // Multiple statements.
            List<String> ddlStmts = multiDdlContext.getDdlStmts();
            if (numOfPlans == ddlStmts.size()) {
                // Use current DDL statement to request leader to perform a job.
                ddlStmt = ddlStmts.get(multiDdlContext.getPlanIndex());
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED,
                    "The number of logical plans " + numOfPlans
                        + " is not equal to the number of DDL statements " + ddlStmts.size());
            }
        } else {
            ddlStmt = executionContext.getOriginSql();
        }

        String ddlStmtWithoutDebugHint = filterOutDebugHint(ddlStmt, executionContext);
        ddlContext.setDdlStmt(ddlStmtWithoutDebugHint);

        ddlContext.addDataPassed(SQLRecord.CONN_ID, executionContext.getConnId());
        ddlContext.addDataPassed(SQLRecord.CLIENT_IP, executionContext.getClientIp());
        ddlContext.addDataPassed(SQLRecord.TX_ID, executionContext.getTxId());
        ddlContext.addDataPassed(SQLRecord.TRACE_ID, executionContext.getTraceId());
        ddlContext.addDataPassed(DdlConstants.TEST_MODE, executionContext.isTestMode());

        ddlContext.setServerVariables(executionContext.getServerVariables());
        ddlContext.setUserDefVariables(executionContext.getUserDefVariables());
        ddlContext.setExtraServerVariables(executionContext.getExtraServerVariables());
        ddlContext.setExtraCmds(executionContext.getExtraCmds());
        ddlContext.setEncoding(executionContext.getEncoding());
        if (executionContext.getTimeZone() != null) {
            ddlContext.setTimeZone(executionContext.getTimeZone().getMySqlTimeZoneName());
        }

        boolean asyncMode = executionContext.getParamManager().getBoolean(ConnectionParams.PURE_ASYNC_DDL_MODE);
        ddlContext.setAsyncMode(asyncMode);

        if (executionContext.getDdlContext() != null && executionContext.getDdlContext().isSubJob()) {
            ddlContext.setIsSubJob(executionContext.getDdlContext().isSubJob());
            ddlContext.setParentJobId(executionContext.getDdlContext().getParentJobId());
            ddlContext.setParentTaskId(executionContext.getDdlContext().getParentTaskId());
            ddlContext.setForRollback(executionContext.getDdlContext().isForRollback());
            ddlContext.setParentDdlContext(executionContext.getDdlContext().getParentDdlContext());
        }

        if (executionContext.getParamManager().getBoolean(ConnectionParams.EXPLAIN_DDL_PHYSICAL_OPERATION)
            || executionContext.getExplain() != null) {
            ddlContext.setExplain(true);
        } else {
            ddlContext.setExplain(false);
        }
        ddlContext.setSqlMode(executionContext.getSqlMode());
        return ddlContext;
    }

    private static String filterOutDebugHint(String origStmt, ExecutionContext executionContext) {
        if (executionContext.getParamManager().getString(ConnectionParams.DDL_ENGINE_DEBUG) != null ||
            TStringUtil.containsIgnoreCase(origStmt, ConnectionProperties.DDL_ENGINE_DEBUG)) {
            // Remove the debug hint to avoid running repeatedly in potentially subsequent recovery.
            return TStringUtil.substringAfterLast(origStmt, "*/");
        }
        return origStmt;
    }

    public DdlContext copy() {
        DdlContext res = new DdlContext();
        res.setJobId(getJobId());
        res.setDdlType(getDdlType());
        res.setSchemaName(getSchemaName());
        res.setObjectName(getObjectName());
        res.setTraceId(getTraceId());
        res.setDdlStmt(getDdlStmt());
        res.setPausedPolicy(getPausedPolicy());
        res.setRollbackPausedPolicy(getRollbackPausedPolicy());

        GeneralUtil.addAllIfNotEmpty(getResources(), res.resources);
        GeneralUtil.addAllIfNotEmpty(physicalDdlInjectionFlag, res.physicalDdlInjectionFlag);
        res.state.set(getState());
        res.interrupted.set(isInterrupted());

        res.setEnableTrace(isEnableTrace());
        res.setResponseNode(getResponseNode());
        res.setAsyncMode(isAsyncMode());
        res.setUsingWarning(isUsingWarning());

        res.setExplain(getExplain());
        res.setSqlMode(getSqlMode());

        GeneralUtil.addAllIfNotEmpty(getDataPassed(), res.dataPassed);
        GeneralUtil.addAllIfNotEmpty(getServerVariables(), res.serverVariables);
        GeneralUtil.addAllIfNotEmpty(getUserDefVariables(), res.userDefVariables);
        GeneralUtil.addAllIfNotEmpty(getExtraServerVariables(), res.extraServerVariables);
        GeneralUtil.addAllIfNotEmpty(getExtraCmds(), res.extraCmds);

        res.setEncoding(getEncoding());
        res.setTimeZone(getTimeZone());
        res.setParentDdlContext(getParentDdlContext());
        res.setForeignKeyOriginalSql(getForeignKeyOriginalSql());

        return res;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public DdlType getDdlType() {
        return ddlType;
    }

    public void setDdlType(DdlType ddlType) {
        this.ddlType = ddlType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public Set<String> getResources() {
        return resources;
    }

    public void setResources(Set<String> resources) {
        this.resources = resources;
    }

    public String getDdlStmt() {
        return ddlStmt;
    }

    public void setDdlStmt(String ddlStmt) {
        this.ddlStmt = ddlStmt;
    }

    public String getResponseNode() {
        return responseNode;
    }

    public void setResponseNode(String responseNode) {
        this.responseNode = responseNode;
    }

    public boolean isEnableTrace() {
        return this.enableTrace;
    }

    public void setEnableTrace(final boolean enableTrace) {
        this.enableTrace = enableTrace;
    }

    public boolean isAsyncMode() {
        return asyncMode;
    }

    public void setAsyncMode(boolean asyncMode) {
        this.asyncMode = asyncMode;
    }

    public boolean isUsingWarning() {
        return usingWarning;
    }

    public void setUsingWarning(boolean usingWarning) {
        this.usingWarning = usingWarning;
    }

    public void addDataPassed(String key, Object value) {
        dataPassed.put(key, value);
    }

    public Map<String, Object> getDataPassed() {
        return dataPassed;
    }

    public void setDataPassed(Map<String, Object> dataPassed) {
        this.dataPassed = dataPassed;
    }

    public Map<String, Object> getUserDefVariables() {
        return this.userDefVariables;
    }

    public void setUserDefVariables(final Map<String, Object> userDefVariables) {
        this.userDefVariables = userDefVariables;
    }

    public Map<String, Object> getServerVariables() {
        return this.serverVariables;
    }

    public void setServerVariables(final Map<String, Object> serverVariables) {
        this.serverVariables = serverVariables;
    }

    public Map<String, Object> getExtraServerVariables() {
        return this.extraServerVariables;
    }

    public void setExtraServerVariables(final Map<String, Object> extraServerVariables) {
        this.extraServerVariables = extraServerVariables;
    }

    public Map<String, Object> getExtraCmds() {
        return extraCmds;
    }

    public void setExtraCmds(Map<String, Object> extraCmds) {
        this.extraCmds = extraCmds;
    }

    public DdlState getState() {
        return this.state.get();
    }

    public void unSafeSetDdlState(final DdlState update) {
        this.state.set(update);
    }

    public Boolean isClientConnectionReset() {
        return this.clientConnectionReset.get();
    }

    public void setClientConnectionResetAsTrue() {
        this.clientConnectionReset.set(true);
    }

    /**
     * true:  current DDL JOB is interrupted, all tasks should stop
     * false: everything is cool
     */
    public Boolean isInterrupted() {
        if (this.interrupted.get()) {
            return true;
        }
        if (jobLease != null && jobLease.get() != null && jobLease.get().inValid()) {
            return true;
        }
        return false;
    }

    public Boolean isRollbackToReady() {
        return this.rollbackToReady.get();
    }

    public AtomicReference<LeaseRecord> getJobLease() {
        return this.jobLease;
    }

    public void setInterruptedAsTrue() {
        this.interrupted.set(true);
    }

    public void setInterruptedAsFalse() {
        this.interrupted.set(false);
    }

    public void setRollbackToReadyAsTrue() {
        this.rollbackToReady.set(true);
    }

    public void setRollbackToReadyAsFalse() {
        this.rollbackToReady.set(false);
    }

    public boolean compareAndSetPhysicalDdlInjectionFlag(long taskId) {
        physicalDdlInjectionFlag.putIfAbsent(taskId, new AtomicBoolean(false));
        return physicalDdlInjectionFlag.get(taskId).compareAndSet(false, true);
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public boolean isSubJob() {
        return this.isSubJob;
    }

    public void setIsSubJob(boolean value) {
        this.isSubJob = value;
    }

    public long getParentJobId() {
        return this.parentJobId;
    }

    public void setParentJobId(final long parentJobId) {
        this.parentJobId = parentJobId;
    }

    public long getParentTaskId() {
        return this.parentTaskId;
    }

    public void setParentTaskId(final long parentTaskId) {
        this.parentTaskId = parentTaskId;
    }

    public boolean isForRollback() {
        return this.forRollback;
    }

    public void setForRollback(final boolean forRollback) {
        this.forRollback = forRollback;
    }

    public boolean isFkRepartition() {
        return this.isFkRepartition;
    }

    public void setFkRepartition(final boolean isFkRepartition) {
        this.isFkRepartition = isFkRepartition;
    }

    public DdlContext getParentDdlContext() {
        return parentDdlContext;
    }

    public void setParentDdlContext(DdlContext parentDdlContext) {
        this.parentDdlContext = parentDdlContext;
    }

    public Boolean getExplain() {
        return explain;
    }

    public void setExplain(Boolean explain) {
        this.explain = explain;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;
    }

    public boolean isIgnoreCdcGsiMark() {
        return ignoreCdcGsiMark;
    }

    public void setIgnoreCdcGsiMark(boolean ignoreCdcGsiMark) {
        this.ignoreCdcGsiMark = ignoreCdcGsiMark;
    }

    public DdlState getPausedPolicy() {
        return pausedPolicy;
    }

    public void setPausedPolicy(DdlState pausedPolicy) {
        this.pausedPolicy = pausedPolicy;
    }

    public DdlState getRollbackPausedPolicy() {
        return rollbackPausedPolicy;
    }

    public void setRollbackPausedPolicy(DdlState rollbackPausedPolicy) {
        this.rollbackPausedPolicy = rollbackPausedPolicy;
    }

    public void setForeignKeyOriginalSql(String foreignKeyOriginalSql) {
        this.foreignKeyOriginalSql = foreignKeyOriginalSql;
    }

    public String getForeignKeyOriginalSql() {
        return this.foreignKeyOriginalSql;
    }
}
