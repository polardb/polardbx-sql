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

import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.Job.JobPhase;
import com.alibaba.polardbx.common.ddl.Job.JobSource;
import com.alibaba.polardbx.common.ddl.Job.JobState;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.encrypt.MD5Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.ddl.Job.JOB_COMPOUND;
import static com.alibaba.polardbx.common.ddl.Job.JOB_SEPARATE;

public class AsyncDDLContext {

    private String schemaName = null;

    private Map<String, Object> extraData = null;
    private ParamManager paramManager = new ParamManager(new HashMap());

    private Set<String> errorHashesIgnored = null;

    private Job job = null;

    private boolean fenceExempted = false;
    private int phyObjsTotal = 0;
    private AtomicInteger phyObjsDone = new AtomicInteger(0);
    private boolean jobNameSwitched = false;

    private Map<String, String> objectsDoneWithHash = null;

    private String parentObjectName = null;

    private Pair<Boolean, String> specialForNoPending = null;
    private boolean allUnknownTableErrors = false;

    private boolean asyncDDLSupported = false;
    private boolean asyncDDLPureMode = false;
    private Boolean asyncDDLPureModeSession = false;

    private String traceId;
    private long txId = 0L;
    private long connId;
    private String clientIp;
    private boolean testMode;

    // For logging
    private String ddlTypeLog;
    private long lastTimingPoint = 0L;

    public AsyncDDLContext() {
    }

    public static boolean isValidJob(Job job) {
        return job != null && job.getId() > JOB_COMPOUND;
    }

    public static boolean isSeparateJob(Job job) {
        return isValidJob(job) && job.getParentId() == JOB_SEPARATE;
    }

    public static boolean isParentJob(Job job) {
        return isValidJob(job) && job.getParentId() == JOB_COMPOUND;
    }

    public static boolean isSubJob(Job job) {
        return isValidJob(job) && job.getParentId() > JOB_COMPOUND;
    }

    public boolean isValidJob() {
        return isValidJob(job);
    }

    public boolean isSeparateJob() {
        return isSeparateJob(job);
    }

    public boolean isParentJob() {
        return isParentJob(job);
    }

    public boolean isSubJob() {
        return isSubJob(job);
    }

    public void resetParentJob() {
        job.setId(job.getParentId());
        job.resetParentId();
        job.setPhase(JobPhase.EXECUTE);
        job.setState(JobState.PENDING);
    }

    public void resetSubJob() {
        Job job = this.job.renew();
        job.setParentId(this.job.getId());
        job.resetId();
        job.resetSeq();
        this.job = job;
    }

    public void renewSubJob() {
        this.job = this.job.renew();
    }

    public void resetJob() {
        if (this.job != null) {
            this.job.resetJob();
        }
    }

    public boolean isJobNew() {
        return job != null && job.getSource() == JobSource.NEW;
    }

    public boolean isJobNormal() {
        return job != null && job.getSource() == JobSource.NORMAL;
    }

    public boolean isJobRecovered() {
        return job != null && job.getSource() == JobSource.RECOVERY;
    }

    public boolean isJobRolledBack() {
        return job != null && job.getSource() == JobSource.ROLLBACK;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Map<String, Object> getExtraData() {
        return extraData;
    }

    public void setExtraData(Map<String, Object> extraData) {
        this.extraData = extraData;
    }

    public ParamManager getParamManager() {
        return paramManager;
    }

    public void setParamManager(ParamManager paramManager) {
        this.paramManager = paramManager;
    }

    public boolean checkIfErrorIgnored(ExecutionContext.ErrorMessage errorMessage) {
        String errorHashIgnored = MD5Utils.getInstance().getMD5String(errorMessage.getGroupName()
            + errorMessage.getCode()
            + errorMessage.getMessage());
        return errorHashesIgnored != null && errorHashesIgnored.contains(errorHashIgnored);
    }

    public void addErrorIgnored(ExecutionContext.ErrorMessage errorMessage) {
        if (this.errorHashesIgnored == null) {
            synchronized (this) {
                if (this.errorHashesIgnored == null) {
                    this.errorHashesIgnored = ConcurrentHashMap.newKeySet();
                }
            }
        }
        String errorHashIgnored = MD5Utils.getInstance().getMD5String(errorMessage.getGroupName()
            + errorMessage.getCode()
            + errorMessage.getMessage());
        this.errorHashesIgnored.add(errorHashIgnored);
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public boolean isFenceExempted() {
        return fenceExempted;
    }

    public void setFenceExempted(boolean fenceExempted) {
        this.fenceExempted = fenceExempted;
    }

    public int getPhyObjsTotal() {
        return phyObjsTotal;
    }

    public void setPhyObjsTotal(int phyObjsTotal) {
        this.phyObjsTotal = phyObjsTotal;
    }

    public int getPhyObjsDone() {
        return phyObjsDone.get();
    }

    public void setPhyObjsDone(int phyObjsDone) {
        this.phyObjsDone.set(phyObjsDone);
    }

    public void increasePhyObjsDone() {
        this.phyObjsDone.incrementAndGet();
    }

    public void decreasePhyObjsDone() {
        this.phyObjsDone.decrementAndGet();
    }

    public boolean isJobNameSwitched() {
        return jobNameSwitched;
    }

    public void setJobNameSwitched(boolean jobNameSwitched) {
        this.jobNameSwitched = jobNameSwitched;
    }

    public Map<String, String> getObjectsDoneWithHash() {
        return objectsDoneWithHash;
    }

    public void setObjectsDoneWithHash(Map<String, String> objectsDoneWithHash) {
        this.objectsDoneWithHash = objectsDoneWithHash;
    }

    public String getParentObjectName() {
        return parentObjectName;
    }

    public void setParentObjectName(String parentObjectName) {
        this.parentObjectName = parentObjectName;
    }

    public Pair<Boolean, String> getSpecialForNoPending() {
        return specialForNoPending;
    }

    public void setSpecialForNoPending(Pair<Boolean, String> specialForNoPending) {
        this.specialForNoPending = specialForNoPending;
    }

    public boolean isAllUnknownTableErrors() {
        return allUnknownTableErrors;
    }

    public void setAllUnknownTableErrors(boolean allUnknownTableErrors) {
        this.allUnknownTableErrors = allUnknownTableErrors;
    }

    public boolean isAsyncDDLSupported() {
        return asyncDDLSupported;
    }

    public void setAsyncDDLSupported(boolean asyncDDLSupported) {
        this.asyncDDLSupported = asyncDDLSupported;
    }

    public boolean isAsyncDDLPureMode() {
        return isAsyncDDLSupported() && asyncDDLPureMode;
    }

    public void setAsyncDDLPureMode(boolean asyncDDLPureMode) {
        this.asyncDDLPureMode = asyncDDLPureMode;
    }

    public Boolean getAsyncDDLPureModeSession() {
        return asyncDDLPureModeSession;
    }

    public void setAsyncDDLPureModeSession(Boolean asyncDDLPureModeSession) {
        this.asyncDDLPureModeSession = asyncDDLPureModeSession;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public long getTxId() {
        return txId;
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public long getConnId() {
        return connId;
    }

    public void setConnId(long connId) {
        this.connId = connId;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public boolean isTestMode() {
        return testMode;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    public void setDdlTypeForLog(String ddlTypeForLog) {
        this.ddlTypeLog = ddlTypeForLog;
        if (isJobRolledBack()) {
            this.ddlTypeLog += "(ROLLBACK)";
        } else if (isJobRecovered()) {
            this.ddlTypeLog += "(RECOVERY)";
        }
    }

    public void setLastTimingPoint(long lastTimingPoint) {
        this.lastTimingPoint = lastTimingPoint;
    }

    public long calculateLastDuration() {
        long lastDuration = System.nanoTime() / 1000_000 - this.lastTimingPoint;
        this.lastTimingPoint += lastDuration;
        return lastDuration;
    }

    public String outputLog(boolean successful) {
        if (job == null || TStringUtil.isEmpty(ddlTypeLog)) {
            return null;
        }
        StringBuilder log = new StringBuilder();
        recordLog(log, successful ? "SUCCESS" : "FAILURE");
        recordLog(log, job.getObjectSchema());
        recordLog(log, job.getObjectName());
        recordLog(log, ddlTypeLog);
        recordLog(log, calculateLastDuration());
        recordLog(log, traceId);
        recordLog(log, job.getDdlStmt());
        return log.toString();
    }

    private void recordLog(StringBuilder log, Object msg) {
        log.append(msg).append(" # ");
    }

    public AsyncDDLContext copy() {
        AsyncDDLContext asyncDDLContext = new AsyncDDLContext();
        asyncDDLContext.setJobNameSwitched(this.jobNameSwitched);
        asyncDDLContext.setJob(this.job);
        asyncDDLContext.setFenceExempted(this.fenceExempted);
        asyncDDLContext.setAsyncDDLPureMode(this.asyncDDLPureMode);
        asyncDDLContext.setAsyncDDLSupported(this.asyncDDLSupported);
        asyncDDLContext.setSchemaName(this.schemaName);
        asyncDDLContext.setAllUnknownTableErrors(this.allUnknownTableErrors);
        asyncDDLContext.setAsyncDDLPureModeSession(this.asyncDDLPureModeSession);
        asyncDDLContext.setClientIp(this.clientIp);
        asyncDDLContext.setConnId(this.connId);
        asyncDDLContext.setExtraData(this.extraData);
        asyncDDLContext.setParamManager(this.paramManager);
        asyncDDLContext.setPhyObjsDone(this.phyObjsTotal);
        asyncDDLContext.setSpecialForNoPending(this.specialForNoPending);
        asyncDDLContext.setTestMode(this.testMode);
        asyncDDLContext.setTraceId(this.traceId);
        asyncDDLContext.setTxId(this.txId);
        asyncDDLContext.setObjectsDoneWithHash(this.objectsDoneWithHash);
        asyncDDLContext.setDdlTypeForLog(this.ddlTypeLog);
        asyncDDLContext.setLastTimingPoint(this.lastTimingPoint);
        return asyncDDLContext;
    }

}
