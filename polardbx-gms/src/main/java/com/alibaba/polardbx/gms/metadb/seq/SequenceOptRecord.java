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

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_MAX_VALUE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NA;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NOCYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.TIME_BASED;

public class SequenceOptRecord extends SequenceBaseRecord {

    public SequenceOptRecord() {
        fillDefault();
    }

    public int incrementBy;
    public long startWith;
    public long maxValue;
    public int cycle;
    public int status;

    public boolean cycleReset;

    @Override
    public SequenceOptRecord fill(ResultSet rs) throws SQLException {
        super.fill(rs);
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
        this.cycleReset = true;
    }

    @Override
    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = super.buildInsertParams();
        int index = params.size();
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.incrementBy);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.startWith);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.maxValue);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.cycle);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.status);
        return params;
    }

    public boolean isTimeBased() {
        return cycle != NA && (cycle & TIME_BASED) == TIME_BASED;
    }

    public boolean isNewSeq() {
        return cycle != NA && (cycle & NEW_SEQ) == NEW_SEQ;
    }

    public boolean isOnlyStartWithChanged() {
        return startWith > 0 && incrementBy <= 0 && maxValue <= 0 && !cycleReset;
    }

}
