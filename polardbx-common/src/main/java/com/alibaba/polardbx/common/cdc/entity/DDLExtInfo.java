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

package com.alibaba.polardbx.common.cdc.entity;

import com.alibaba.polardbx.common.cdc.DdlScope;
import lombok.Getter;

/**
 * Created by ziyang.lb
 **/
public class DDLExtInfo {
    /**
     * special for new ddl engine
     */
    private Long taskId;

    /**
     * 为CDC Meta组件提供创建物理表的依据
     */
    private String createSql4PhyTable;

    /**
     * should be set if polardbx_server_id variable is in execution context
     */
    private String serverId;

    /**
     * mark if used OMC with the ddl sql
     */
    private Boolean useOMC;
    /**
     * sub sequence for one task , support for multi mark in one task
     */
    private Long taskSubSeq;
    /**
     * sql mode for logic ddl event, null and empty for this field has different meaning
     */
    private String sqlMode = null;
    private String originalDdl = null;
    private Boolean isGsi = false;
    private String groupName = null;
    @Getter
    private Boolean foreignKeysDdl = false;
    @Getter
    private String flags2;
    private int ddlScope = DdlScope.Schema.getValue();
    private Boolean manuallyCreatedTableGroup;
    private boolean enableImplicitTableGroup;

    @Getter
    private Long ddlId;

    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public String getCreateSql4PhyTable() {
        return createSql4PhyTable;
    }

    public void setCreateSql4PhyTable(String createSql4PhyTable) {
        this.createSql4PhyTable = createSql4PhyTable;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public Long getTaskSubSeq() {
        return taskSubSeq;
    }

    public void setTaskSubSeq(Long taskSubSeq) {
        this.taskSubSeq = taskSubSeq;
    }

    public Boolean getUseOMC() {
        return useOMC;
    }

    public void setUseOMC(Boolean useOMC) {
        this.useOMC = useOMC;
    }

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;
    }

    public String getOriginalDdl() {
        return originalDdl;
    }

    public void setOriginalDdl(String originalDdl) {
        this.originalDdl = originalDdl;
    }

    public Boolean getGsi() {
        return isGsi;
    }

    public void setGsi(Boolean gsi) {
        isGsi = gsi;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setForeignKeysDdl(Boolean foreignKeysDdl) {
        this.foreignKeysDdl = foreignKeysDdl;
    }

    public void setFlags2(String flags2) {
        this.flags2 = flags2;
    }

    public int getDdlScope() {
        return ddlScope;
    }

    public void setDdlScope(int ddlScope) {
        this.ddlScope = ddlScope;
    }

    public Boolean getManuallyCreatedTableGroup() {
        return manuallyCreatedTableGroup;
    }

    public void setManuallyCreatedTableGroup(Boolean manuallyCreatedTableGroup) {
        this.manuallyCreatedTableGroup = manuallyCreatedTableGroup;
    }

    public boolean isEnableImplicitTableGroup() {
        return enableImplicitTableGroup;
    }

    public void setEnableImplicitTableGroup(boolean enableImplicitTableGroup) {
        this.enableImplicitTableGroup = enableImplicitTableGroup;
    }

    public void setDdlId(Long ddlId) {
        this.ddlId = ddlId;
    }
}
