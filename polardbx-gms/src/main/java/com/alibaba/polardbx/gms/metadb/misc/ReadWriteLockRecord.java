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

package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ReadWriteLockRecord implements SystemTableRecord {

    public String schemaName;
    public String owner;
    public String resource;
    public String type;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;

    @Override
    public ReadWriteLockRecord fill(ResultSet rs) throws SQLException {
        this.schemaName = rs.getString("schema_name");
        this.owner = rs.getString("owner");
        this.resource = rs.getString("resource");
        this.type = rs.getString("type");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.owner);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.resource);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.type);
        return params;
    }

}
