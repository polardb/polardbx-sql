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

package com.alibaba.polardbx.common.ddl;

public class Job {

    public enum JobSource {
        NEW, NORMAL, RECOVERY, ROLLBACK
    }

    public enum JobType {
        UNSUPPORTED, CREATE_TABLE, ALTER_TABLE, DROP_TABLE, RENAME_TABLE, TRUNCATE_TABLE, CREATE_INDEX, DROP_INDEX,
        CREATE_GLOBAL_INDEX, ALTER_GLOBAL_INDEX, DROP_GLOBAL_INDEX, RENAME_GLOBAL_INDEX, CHECK_GLOBAL_INDEX,
        CHECK_COLUMNAR_INDEX, MOVE_TABLE, ALTER_TABLEGROUP, DRAIN_NODE, ALTER_TABLE_SET_TABLEGROUP,
        ALTER_TABLEGROUP_ADD_TABLE,
        MERGE_TABLEGROUP
    }

    public enum JobPhase {
        INIT, PREPARE, EXECUTE, MERGE, VERIFY, CLEANUP
    }

    public enum JobState {
        INITIALIZED, PREPARED, RUNNING, EXECUTED, PENDING, MERGED, VERIFIED, STAGED, COMPLETED
    }

    public static final long JOB_SEPARATE = 0L;
    public static final long JOB_COMPOUND = 1L;

    /**
     * Bit-map flags from the RESERVED_DDL_INT field.
     */
    public static final int FLAG_NONE = 0;
    public static final int FLAG_ROLLBACK = 1;
    public static final int FLAG_ASYNC = 2;
    public static final int FLAG_CANCELLED = 4;

    private long id = JOB_SEPARATE;
    private long parentId = JOB_SEPARATE;
    private int seq = 0;
    private String server = null;
    private String objectSchema = null;
    private String objectName = null;
    private String newObjectName = null;
    private JobSource source = JobSource.NEW;
    private JobType type = JobType.UNSUPPORTED;
    private JobPhase phase = JobPhase.INIT;
    private JobState state = JobState.INITIALIZED;
    private String physicalObjectDone = null;
    private int progress = 0;
    private String ddlStmt = null;
    private String gmtCreated = null;
    private String gmtModified = null;
    private long elapsedTime = 0L;
    private String remark = null;
    private String backfillProgress = null;
    private int flag = 0;

    public Job() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void resetId() {
        this.id = JOB_SEPARATE;
    }

    public long getParentId() {
        return parentId;
    }

    public void setParentId(long parentId) {
        this.parentId = parentId;
    }

    public void resetParentId() {
        this.parentId = JOB_COMPOUND;
    }

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    public int incrementSeq() {
        return ++this.seq;
    }

    public void resetSeq() {
        this.seq = 0;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getObjectSchema() {
        return objectSchema;
    }

    public void setObjectSchema(String objectSchema) {
        this.objectSchema = objectSchema;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getNewObjectName() {
        return newObjectName;
    }

    public void setNewObjectName(String newObjectName) {
        this.newObjectName = newObjectName;
    }

    public JobSource getSource() {
        return source;
    }

    public void setSource(JobSource source) {
        this.source = source;
    }

    public void resetSource() {
        this.source = JobSource.NEW;
    }

    public JobType getType() {
        return type;
    }

    public void setType(JobType type) {
        this.type = type;
    }

    public JobPhase getPhase() {
        return phase;
    }

    public void setPhase(JobPhase phase) {
        this.phase = phase;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public String getPhysicalObjectDone() {
        return physicalObjectDone;
    }

    public void setPhysicalObjectDone(String physicalObjectDone) {
        this.physicalObjectDone = physicalObjectDone;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public String getDdlStmt() {
        return ddlStmt;
    }

    public void setDdlStmt(String ddlStmt) {
        this.ddlStmt = ddlStmt;
    }

    public String getGmtCreated() {
        return gmtCreated;
    }

    public void setGmtCreated(String gmtCreated) {
        this.gmtCreated = gmtCreated;
    }

    public String getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(String gmtModified) {
        this.gmtModified = gmtModified;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getBackfillProgress() {
        return backfillProgress;
    }

    public void setBackfillProgress(String backfillProgress) {
        this.backfillProgress = backfillProgress;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag |= flag;
    }

    public boolean hasNoFlag() {
        return flag == FLAG_NONE;
    }

    public boolean hasRollbackFlag() {
        return (flag & FLAG_ROLLBACK) == FLAG_ROLLBACK;
    }

    public boolean hasAsyncModeFlag() {
        return (flag & FLAG_ASYNC) == FLAG_ASYNC;
    }

    public boolean hasCancelledFlag() {
        return (flag & FLAG_CANCELLED) == FLAG_CANCELLED;
    }

    public Job renew() {
        Job job = new Job();
        job.setParentId(getParentId());
        job.setSeq(incrementSeq());
        job.setServer(getServer());
        job.setObjectSchema(getObjectSchema());
        job.setObjectName(getObjectName());
        job.setNewObjectName(getNewObjectName());
        job.setSource(getSource());
        job.setType(getType());
        job.setDdlStmt(getDdlStmt());
        job.setFlag(getFlag());
        return job;
    }

    public void resetJob() {
        this.setId(0);
        this.setParentId(0);
        this.setState(JobState.INITIALIZED);
        this.setPhase(JobPhase.INIT);
        this.setSource(JobSource.NEW);
    }

    @Override
    public Job clone() {
        Job job = new Job();
        job.setId(getId());
        job.setParentId(getParentId());
        job.setSeq(getSeq());
        job.setServer(getServer());
        job.setObjectSchema(getObjectSchema());
        job.setObjectName(getObjectName());
        job.setNewObjectName(getNewObjectName());
        job.setSource(getSource());
        job.setType(getType());
        job.setPhase(getPhase());
        job.setState(getState());
        job.setPhysicalObjectDone(getPhysicalObjectDone());
        job.setProgress(getProgress());
        job.setDdlStmt(getDdlStmt());
        job.setGmtModified(getGmtModified());
        job.setGmtCreated(getGmtCreated());
        job.setElapsedTime(getElapsedTime());
        job.setRemark(getRemark());
        job.setBackfillProgress(getBackfillProgress());
        job.setFlag(getFlag());
        return job;
    }

}
