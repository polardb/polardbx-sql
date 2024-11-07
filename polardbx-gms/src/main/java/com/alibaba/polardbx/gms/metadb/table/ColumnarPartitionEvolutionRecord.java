/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Getter
public class ColumnarPartitionEvolutionRecord implements SystemTableRecord {
    private static final Logger log = LoggerFactory.getLogger(ColumnarPartitionEvolutionRecord.class);

    public long id;
    public long tableId;
    public long partitionId;
    public String partitionName;
    public long versionId;
    public long ddlJobId;
    public int status;

    public Timestamp create;
    public TablePartitionRecord partitionRecord;

    public ColumnarPartitionEvolutionRecord() {
    }

    public ColumnarPartitionEvolutionRecord(long tableId, String partitionName, long versionId, long ddlJobId,
                                            TablePartitionRecord partitionRecord, int status) {
        this.tableId = tableId;
        this.partitionName = partitionName;
        this.versionId = versionId;
        this.ddlJobId = ddlJobId;
        this.partitionRecord = partitionRecord;
        this.status = status;
    }

    public ColumnarPartitionEvolutionRecord(long tableId, long partitionId, String columnName, long versionId,
                                            long ddlJobId, TablePartitionRecord partitionRecord, int status) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partitionName = columnName;
        this.versionId = versionId;
        this.ddlJobId = ddlJobId;
        this.partitionRecord = partitionRecord;
        this.status = status;
    }

    public static String serializeToJson(TablePartitionRecord partitionRecord) {
        JSONObject tablePartitionJson = new JSONObject();

        tablePartitionJson.put("id", partitionRecord.id);
        tablePartitionJson.put("parentId", partitionRecord.parentId);
        tablePartitionJson.put("tableSchema", partitionRecord.tableSchema);
        tablePartitionJson.put("tableName", partitionRecord.tableName);
        tablePartitionJson.put("spTempFlag", partitionRecord.spTempFlag);
        tablePartitionJson.put("groupId", partitionRecord.groupId);
        tablePartitionJson.put("metaVersion", partitionRecord.metaVersion);
        tablePartitionJson.put("autoFlag", partitionRecord.autoFlag);
        tablePartitionJson.put("tblType", partitionRecord.tblType);
        tablePartitionJson.put("partName", partitionRecord.partName);
        tablePartitionJson.put("partTempName", partitionRecord.partTempName);
        tablePartitionJson.put("partLevel", partitionRecord.partLevel);
        tablePartitionJson.put("nextLevel", partitionRecord.nextLevel);
        tablePartitionJson.put("partStatus", partitionRecord.partStatus);
        tablePartitionJson.put("partPosition", partitionRecord.partPosition);
        tablePartitionJson.put("partMethod", partitionRecord.partMethod);
        tablePartitionJson.put("partExpr", partitionRecord.partExpr);
        tablePartitionJson.put("partDesc", partitionRecord.partDesc);
        tablePartitionJson.put("partComment", partitionRecord.partComment);
        tablePartitionJson.put("partEngine", partitionRecord.partEngine);
        tablePartitionJson.put("partExtras", ExtraFieldJSON.toJson(partitionRecord.partExtras));
        tablePartitionJson.put("partFlags", partitionRecord.partFlags);
        tablePartitionJson.put("phyTable", partitionRecord.phyTable);

        return tablePartitionJson.toJSONString();
    }

    public static TablePartitionRecord deserializeFromJson(String json) {
        TablePartitionRecord partitionRecord = new TablePartitionRecord();
        JSONObject columnRecordJson = JSON.parseObject(json);

        try {
            partitionRecord.id = columnRecordJson.getLongValue("id");
        } catch (Exception e) {
            // id may be not exists in old version
            log.warn(
                "Cannot deserialize id from json because old version columnar_partition_evolution record's id not exist, use default value -1");
            partitionRecord.id = -1L;
        }
        partitionRecord.parentId = columnRecordJson.getLongValue("parentId");
        partitionRecord.tableSchema = columnRecordJson.getString("tableSchema");
        partitionRecord.tableName = columnRecordJson.getString("tableName");
        partitionRecord.spTempFlag = columnRecordJson.getIntValue("spTempFlag");
        partitionRecord.groupId = columnRecordJson.getLongValue("groupId");
        partitionRecord.metaVersion = columnRecordJson.getLongValue("metaVersion");
        partitionRecord.autoFlag = columnRecordJson.getIntValue("autoFlag");
        partitionRecord.tblType = columnRecordJson.getIntValue("tblType");
        partitionRecord.partName = columnRecordJson.getString("partName");
        partitionRecord.partTempName = columnRecordJson.getString("partTempName");
        partitionRecord.partLevel = columnRecordJson.getIntValue("partLevel");
        partitionRecord.nextLevel = columnRecordJson.getIntValue("nextLevel");
        partitionRecord.partStatus = columnRecordJson.getIntValue("partStatus");
        partitionRecord.partPosition = columnRecordJson.getLongValue("partPosition");
        partitionRecord.partMethod = columnRecordJson.getString("partMethod");
        partitionRecord.partExpr = columnRecordJson.getString("partExpr");
        partitionRecord.partDesc = columnRecordJson.getString("partDesc");
        partitionRecord.partComment = columnRecordJson.getString("partComment");
        partitionRecord.partEngine = columnRecordJson.getString("partEngine");
        partitionRecord.partExtras = ExtraFieldJSON.fromJson(columnRecordJson.getString("partExtras"));
        partitionRecord.partFlags = columnRecordJson.getLongValue("partFlags");
        partitionRecord.phyTable = columnRecordJson.getString("phyTable");

        return partitionRecord;
    }

    @Override
    public ColumnarPartitionEvolutionRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.partitionId = rs.getLong("partition_id");
        this.tableId = rs.getLong("table_id");
        this.partitionName = rs.getString("partition_name");
        this.status = rs.getInt("status");
        this.versionId = rs.getLong("version_id");
        this.ddlJobId = rs.getLong("ddl_job_id");
        this.partitionRecord = deserializeFromJson(rs.getString("partition_record"));
        this.create = rs.getTimestamp("gmt_created");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.partitionId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.partitionName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.versionId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ddlJobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, serializeToJson(this.partitionRecord));
        return params;
    }
}
