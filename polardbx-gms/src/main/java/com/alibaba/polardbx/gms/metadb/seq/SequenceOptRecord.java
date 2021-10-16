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

package com.alibaba.polardbx.gms.metadb.seq;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_MAX_VALUE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NOCYCLE;

public class SequenceOptRecord implements SystemTableRecord {

    public SequenceOptRecord() {
        fillDefault();
    }

    public String schemaName;
    public String name;
    public String newName;
    public long value;
    public int incrementBy;
    public long startWith;
    public long maxValue;
    public int cycle;
    public int status;

    @Override
    public SequenceOptRecord fill(ResultSet rs) throws SQLException {
        this.schemaName = rs.getString("schema_name");
        this.name = rs.getString("name");
        this.newName = rs.getString("new_name");
        this.value = rs.getLong("value");
        this.incrementBy = rs.getInt("increment_by");
        this.startWith = rs.getLong("start_with");
        this.maxValue = rs.getLong("max_value");
        this.cycle = rs.getInt("cycle");
        this.status = rs.getInt("status");
        return this;
    }

    public void fillDefault() {
        this.value = DEFAULT_START_WITH;
        this.incrementBy = DEFAULT_INCREMENT_BY;
        this.startWith = DEFAULT_START_WITH;
        this.maxValue = DEFAULT_MAX_VALUE;
        this.cycle = NOCYCLE;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(9);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.name);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.newName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.value);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.incrementBy);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.startWith);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.maxValue);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.cycle);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        return params;
    }

}
