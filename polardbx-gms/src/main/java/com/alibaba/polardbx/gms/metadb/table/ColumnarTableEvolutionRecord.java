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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnarTableEvolutionRecord implements SystemTableRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    public long versionId;
    public long tableId;
    public String tableSchema;
    public String tableName;
    public String indexName;
    public long ddlJobId;
    public String ddlType;
    public long commitTs;
    public List<Long> columns;
    public List<Long> partitions;
    public Map<String, String> options;

    public ColumnarTableEvolutionRecord() {
    }

    public ColumnarTableEvolutionRecord(long versionId, long tableId, String tableSchema, String tableName,
                                        String indexName,
                                        Map<String, String> options,
                                        long ddlJobId,
                                        String ddlType, long commitTs,
                                        List<Long> columns, List<Long> partitions) {
        this.versionId = versionId;
        this.tableId = tableId;
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.indexName = indexName;
        this.options = options;
        this.ddlJobId = ddlJobId;
        this.ddlType = ddlType;
        this.commitTs = commitTs;
        this.columns = columns;
        this.partitions = partitions;
    }

    public static String serializeToJson(List<Long> ids) {
        JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(ids);
        return jsonArray.toJSONString();
    }

    public static String serializeToJson(Map<String, String> options) {
        if (options == null || options.isEmpty()) {
            return null;
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.putAll(options);
        return jsonObject.toJSONString();
    }

    public static List<Long> deserializeListFromJson(String json) {
        if (json == null) {
            return new ArrayList<>();
        }
        List<Long> results = new ArrayList<>();
        JSONArray jsonArray = JSON.parseArray(json);
        for (int i = 0; i < jsonArray.size(); i++) {
            results.add(jsonArray.getLong(i));
        }
        return results;
    }

    public static Map<String, String> deserializeMapFromJson(String json) {
        if (json == null) {
            return null;
        }
        JSONObject jsonObject = JSON.parseObject(json);
        if (jsonObject == null || jsonObject.isEmpty()) {
            return null;
        }
        Map<String, String> results = new HashMap<>(jsonObject.size());
        for (Map.Entry<String, Object> entry : jsonObject.getInnerMap().entrySet()) {
            if (entry.getValue() != null && !(entry.getValue() instanceof String)) {
                String type = entry.getValue().getClass().getName();
                throw new IllegalArgumentException(String.format("Cannot cast type %s to String, value: %s",
                    type, entry.getValue()));
            }
            results.put(entry.getKey(), (String) entry.getValue());
        }
        return results;
    }

    @Override
    public ColumnarTableEvolutionRecord fill(ResultSet rs) throws SQLException {
        this.versionId = rs.getLong("version_id");
        this.tableId = rs.getLong("table_id");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.indexName = rs.getString("index_name");
        this.ddlJobId = rs.getLong("ddl_job_id");
        this.ddlType = rs.getString("ddl_type");
        this.commitTs = rs.getLong("commit_ts");

        String columns = rs.getString("columns");
        this.columns = deserializeListFromJson(columns);

        String partitions = rs.getString("partitions");
        this.partitions = deserializeListFromJson(partitions);

        String options = rs.getString("options");
        this.options = deserializeMapFromJson(options);

        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.versionId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ddlJobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.ddlType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.commitTs);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, serializeToJson(this.columns));
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, serializeToJson(this.partitions));
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, serializeToJson(this.options));
        return params;
    }
}
