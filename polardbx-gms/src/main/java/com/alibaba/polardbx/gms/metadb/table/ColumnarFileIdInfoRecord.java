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
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Data
public class ColumnarFileIdInfoRecord implements SystemTableRecord {
    private long id;
    private String type;
    private long maxId;
    private int step;
    private long version;

    private Timestamp createTime;
    private Timestamp updateTime;

    @Override
    public ColumnarFileIdInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.type = rs.getString("type");
        this.maxId = rs.getLong("max_id");
        this.step = rs.getInt("step");
        this.version = rs.getLong("version");
        this.createTime = rs.getTimestamp("create_time");
        this.updateTime = rs.getTimestamp("update_time");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(7);
        int index = 0;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.type);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.maxId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.step);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.version);
        // skip automatically updated column: create_time and update_time
        return params;
    }

    public long getMaxId() {
        return this.maxId;
    }

    public int getStep() {
        return this.step;
    }

    public long getId() {
        return this.id;
    }

    public long getVersion() {
        return this.version;
    }

    public String getType() {
        return this.type;
    }
}
