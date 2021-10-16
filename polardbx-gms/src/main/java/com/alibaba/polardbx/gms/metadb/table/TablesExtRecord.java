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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TablesExtRecord implements SystemTableRecord {

    public static final long FLAG_LOCK = 0x1;
    public static final long FLAG_AUTO_PARTITION = 0x2;

    public String tableSchema;
    public String tableName;
    public String newTableName;
    public int tableType;
    public String dbPartitionKey;
    public String dbPartitionPolicy;
    public int dbPartitionCount;
    public String dbNamePattern;
    public String dbRule;
    public String dbMetaMap;
    public String tbPartitionKey;
    public String tbPartitionPolicy;
    public int tbPartitionCount;
    public String tbNamePattern;
    public String tbRule;
    public String tbMetaMap;
    public String extPartitions;
    public int fullTableScan;
    public int broadcast;
    public long version;
    public int status;
    public long flag;

    @Override
    public TablesExtRecord fill(ResultSet rs) throws SQLException {
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.newTableName = rs.getString("new_table_name");
        this.tableType = rs.getInt("table_type");
        this.dbPartitionKey = rs.getString("db_partition_key");
        this.dbPartitionPolicy = rs.getString("db_partition_policy");
        this.dbPartitionCount = rs.getInt("db_partition_count");
        this.dbNamePattern = rs.getString("db_name_pattern");
        this.dbRule = rs.getString("db_rule");
        this.dbMetaMap = rs.getString("db_meta_map");
        this.tbPartitionKey = rs.getString("tb_partition_key");
        this.tbPartitionPolicy = rs.getString("tb_partition_policy");
        this.tbPartitionCount = rs.getInt("tb_partition_count");
        this.tbNamePattern = rs.getString("tb_name_pattern");
        this.tbRule = rs.getString("tb_rule");
        this.tbMetaMap = rs.getString("tb_meta_map");
        this.extPartitions = rs.getString("ext_partitions");
        this.fullTableScan = rs.getInt("full_table_scan");
        this.broadcast = rs.getInt("broadcast");
        this.version = rs.getLong("version");
        this.status = rs.getInt("status");
        this.flag = rs.getLong("flag");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(22);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.newTableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.tableType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.dbPartitionKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.dbPartitionPolicy);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.dbPartitionCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.dbNamePattern);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.dbRule);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.dbMetaMap);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tbPartitionKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tbPartitionPolicy);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.tbPartitionCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tbNamePattern);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tbRule);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tbMetaMap);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extPartitions);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.fullTableScan);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.broadcast);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.version);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.flag);
        return params;
    }

    public boolean isLocked() {
        return (flag & FLAG_LOCK) != 0L;
    }

    public void setLock() {
        flag |= FLAG_LOCK;
    }

    public void clearLock() {
        flag &= ~FLAG_LOCK;
    }

    public boolean isAutoPartition() {
        return (flag & FLAG_AUTO_PARTITION) != 0L;
    }

    public void setAutoPartition() {
        flag |= FLAG_AUTO_PARTITION;
    }

    public void clearAutoPartition() {
        flag &= ~FLAG_AUTO_PARTITION;
    }

    public String getTableName() {
        return this.tableName;
    }
}
