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

package com.alibaba.polardbx.cdc.entity;

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
}
