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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class GsiStatisticsRecord implements SystemTableRecord {

    public Long id;

    public String indexSchema;

    public String tableName;

    public String indexName;

    public Long visitFrequency;

    public Date lastAccessTime;

    @Override
    public GsiStatisticsRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.indexSchema = rs.getString("index_schema");
        this.tableName = rs.getString("table_name");
        this.indexName = rs.getString("index_name");
        this.visitFrequency = rs.getLong("visit_frequency");
        this.lastAccessTime = rs.getTimestamp("last_access_time");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexName);
        if (this.visitFrequency != null) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.visitFrequency);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, this.visitFrequency);
        }

        if (this.lastAccessTime != null) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setTimestamp1, this.lastAccessTime);
        } else {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setNull1, this.lastAccessTime);
        }
        return params;
    }

}
