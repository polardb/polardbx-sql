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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;

public class SequenceRecord extends SequenceBaseRecord {

    public SequenceRecord() {
        fillDefault();
    }

    public int unitCount;
    public int unitIndex;
    public int innerStep;
    public int status;

    @Override
    public SequenceRecord fill(ResultSet rs) throws SQLException {
        super.fill(rs);
        this.unitCount = rs.getInt("unit_count");
        this.unitIndex = rs.getInt("unit_index");
        this.innerStep = rs.getInt("inner_step");
        this.status = rs.getInt("status");
        return this;
    }

    public void fillDefault() {
        this.value = 0;
        this.unitCount = DEFAULT_UNIT_COUNT;
        this.unitIndex = DEFAULT_UNIT_INDEX;
        this.innerStep = DEFAULT_INNER_STEP;
    }

    @Override
    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = super.buildInsertParams();
        int index = params.size();
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.unitCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.unitIndex);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.innerStep);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        return params;
    }

}
