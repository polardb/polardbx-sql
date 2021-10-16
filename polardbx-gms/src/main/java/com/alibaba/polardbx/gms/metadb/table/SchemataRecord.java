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

public class SchemataRecord implements SystemTableRecord {

    public String schemaName;
    public String defaultCharSetName;
    public String defaultCollationName;
    public String defaultDbIndex;

    @Override
    public SchemataRecord fill(ResultSet rs) throws SQLException {
        this.schemaName = rs.getString("schema_name");
        this.defaultCharSetName = rs.getString("default_character_set_name");
        this.defaultCollationName = rs.getString("default_collation_name");
        this.defaultDbIndex = rs.getString("default_db_index");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(5);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.defaultCharSetName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.defaultCollationName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.defaultDbIndex);
        return params;
    }

}
