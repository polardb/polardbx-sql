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
@Deprecated
public class TableGroupOutlineRecord implements SystemTableRecord {
    private long id;
    private long batch_id;
    private long job_id;
    private long parent_job_id;
    private Date gmtCreate;
    private Date gmtModified;
    private String schema_name;
    private long table_group_id;
    private String partition_group_name;
    private String table_name;
    //{"table_group_name":"xxx","type":"split", "old_partition":"xx",
    // "ordered_locations":{"groupNum":"xxx","group1":"xx","group2":"xx",..,"groupN":xx},
    //"newPartition":{"num":"xx","p1":{"name":"xx"}, "p2":{"name":"xx"}}}
    private String partition_info;
    private int type;
    private int status;
    private long version;
    private String extra;
    private String sourceSql;

    @Override
    public TableGroupOutlineRecord fill(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getLong("id");
        this.batch_id = resultSet.getLong("batch_id");
        this.job_id = resultSet.getLong("job_id");
        this.parent_job_id = resultSet.getLong("parent_job_id");
        this.gmtCreate = resultSet.getTimestamp("gmt_create");
        this.gmtModified = resultSet.getTimestamp("gmt_modified");
        this.schema_name = resultSet.getString("schema_name");
        this.table_group_id = resultSet.getLong("table_group_id");
        this.partition_group_name = resultSet.getString("partition_group_name");
        this.table_name = resultSet.getString("table_name");
        this.partition_info = resultSet.getString("partition_info");
        this.type = resultSet.getInt("type");
        this.status = resultSet.getInt("status");
        this.version = resultSet.getLong("version");
        this.extra = resultSet.getString("extra");
        this.sourceSql = resultSet.getString("source_sql");
        return this;
    }

    public TableGroupOutlineRecord() {

    }

    public TableGroupOutlineRecord(long id, long batch_id, long job_id, long parent_job_id, Date gmtCreate,
                                   Date gmtModified, String schema_name, long table_group_id,
                                   String partition_group_name, String table_name, String partition_info, int type,
                                   int status, long version, String extra, String sourceSql) {
        this.id = id;
        this.batch_id = batch_id;
        this.job_id = job_id;
        this.parent_job_id = parent_job_id;
        this.gmtCreate = gmtCreate;
        this.gmtModified = gmtModified;
        this.schema_name = schema_name;
        this.table_group_id = table_group_id;
        this.partition_group_name = partition_group_name;
        this.table_name = table_name;
        this.partition_info = partition_info;
        this.type = type;
        this.status = status;
        this.version = version;
        this.extra = extra;
        this.sourceSql = sourceSql;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getBatch_id() {
        return batch_id;
    }

    public void setBatch_id(long batch_id) {
        this.batch_id = batch_id;
    }

    public long getJob_id() {
        return job_id;
    }

    public void setJob_id(long job_id) {
        this.job_id = job_id;
    }

    public long getParent_job_id() {
        return parent_job_id;
    }

    public void setParent_job_id(long parent_job_id) {
        this.parent_job_id = parent_job_id;
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

    public String getSchema_name() {
        return schema_name;
    }

    public void setSchema_name(String schema_name) {
        this.schema_name = schema_name;
    }

    public long getTable_group_id() {
        return table_group_id;
    }

    public void setTable_group_id(long table_group_id) {
        this.table_group_id = table_group_id;
    }

    public String getPartition_group_name() {
        return partition_group_name;
    }

    public void setPartition_group_name(String partition_group_name) {
        this.partition_group_name = partition_group_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getPartition_info() {
        return partition_info;
    }

    public void setPartition_info(String partition_info) {
        this.partition_info = partition_info;
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

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
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
}
