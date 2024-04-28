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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Data
public class ColumnarFileMappingRecord implements SystemTableRecord {
    private long id;
    private int columnarFileId;
    private String fileName;
    private String logicalSchema;
    private String logicalTable;
    private String logicalPartition;
    private int level;
    private byte[] smallestKey;
    private byte[] largestKey;
    private String createTime;
    private String updateTime;

    @Override
    public ColumnarFileMappingRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.columnarFileId = rs.getInt("columnar_file_id");
        this.fileName = rs.getString("file_name");
        this.logicalSchema = rs.getString("logical_schema");
        this.logicalTable = rs.getString("logical_table");
        this.logicalPartition = rs.getString("logical_partition");
        this.level = rs.getInt("level");
        this.smallestKey = rs.getBytes("smallest_key");
        this.largestKey = rs.getBytes("largest_key");
        this.createTime = rs.getString("create_time");
        this.updateTime = rs.getString("update_time");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(12);
        int index = 0;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.columnarFileId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.fileName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalTable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.logicalPartition);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.level);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBytes, this.smallestKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBytes, this.largestKey);
        // skip automatically updated column: create_time and update_time
        return params;
    }
}
