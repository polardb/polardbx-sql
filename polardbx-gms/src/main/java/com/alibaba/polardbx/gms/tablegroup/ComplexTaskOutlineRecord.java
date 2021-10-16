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

package com.alibaba.polardbx.gms.tablegroup;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ComplexTaskOutlineRecord implements SystemTableRecord {
    private long id;
    private long job_id;
    private Date gmtCreate;
    private Date gmtModified;
    private String tableSchema;
    private String tableGroupName;
    private String objectName;
    private int type;
    private int status;
    private String extra;
    private String sourceSql;
    private int subTask;

    @Override
    public ComplexTaskOutlineRecord fill(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getLong("id");
        this.job_id = resultSet.getLong("job_id");
        this.gmtCreate = resultSet.getTimestamp("gmt_create");
        this.gmtModified = resultSet.getTimestamp("gmt_modified");
        this.tableSchema = resultSet.getString("table_schema");
        this.tableGroupName = resultSet.getString("tg_name");
        this.objectName = resultSet.getString("object_name");
        this.type = resultSet.getInt("type");
        this.status = resultSet.getInt("status");
        this.extra = resultSet.getString("extra");
        this.sourceSql = resultSet.getString("source_sql");
        this.subTask = resultSet.getInt("sub_task");
        return this;
    }

    public ComplexTaskOutlineRecord() {

    }

    public ComplexTaskOutlineRecord(long id, long job_id, Date gmtCreate, Date gmtModified,
                                    String tableSchema, String tableGroupName, String objectName,
                                    int type, int status, String extra, String sourceSql, int subTask) {
        this.id = id;
        this.job_id = job_id;
        this.gmtCreate = gmtCreate;
        this.gmtModified = gmtModified;
        this.tableSchema = tableSchema;
        this.tableGroupName = tableGroupName;
        this.objectName = objectName;
        this.type = type;
        this.status = status;
        this.extra = extra;
        this.sourceSql = sourceSql;
        this.subTask = subTask;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getJob_id() {
        return job_id;
    }

    public void setJob_id(long job_id) {
        this.job_id = job_id;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public int getSubTask() {
        return subTask;
    }

    public void setSubTask(int subTask) {
        this.subTask = subTask;
    }
}
