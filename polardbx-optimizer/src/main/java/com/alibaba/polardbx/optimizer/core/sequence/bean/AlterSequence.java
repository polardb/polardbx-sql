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

package com.alibaba.polardbx.optimizer.core.sequence.bean;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.optimizer.core.sequence.sequence.IAlterSequence;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;

/**
 * alter sequence
 *
 * @author agapple 2014年12月18日 下午7:00:51
 * @since 5.1.17
 */
public abstract class AlterSequence extends Sequence<IAlterSequence> implements IAlterSequence {

    protected Type type = Type.NA;
    protected long start = SequenceAttribute.NA;
    protected int increment = SequenceAttribute.NA;
    protected long maxValue = SequenceAttribute.NA;
    protected int cycle = SequenceAttribute.NA;

    public AlterSequence(String schemaName, String name) {
        this.name = name;
        this.schemaName = schemaName;
    }

    @Override
    public String toStringWithInden(int inden, ExplainResult.ExplainMode mode) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SEQUENCE ").append(name);
        if (type != Type.NA) {
            sb.append(" CHANGE TO ").append(type.getKeyword());
        }
        if (start > 0) {
            sb.append(" START WITH ").append(start);
        }
        if (increment > 0) {
            sb.append(" INCREMENT BY ").append(increment);
        }
        if (maxValue > 0) {
            sb.append(" MAXVALUE ").append(maxValue);
        }
        if (cycle == SequenceAttribute.NOCYCLE) {
            sb.append(" NOCYCLE");
        } else if (cycle == SequenceAttribute.CYCLE) {
            sb.append(" CYCLE");
        }
        return sb.toString();
    }

    protected abstract AlterSequence getAlterSequence(String schemaName, String name);

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public int getIncrement() {
        return increment;
    }

    public void setIncrement(int increment) {
        this.increment = increment;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
    }

    public int getCycle() {
        return cycle;
    }

    public void setCycle(int cycle) {
        this.cycle = cycle;
    }

    @Override
    public SEQUENCE_DDL_TYPE getSequenceDdlType() {
        return SEQUENCE_DDL_TYPE.ALTER_SEQUENCE;
    }

    @Override
    public String getSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE INTO ").append(SequenceAttribute.DEFAULT_GROUP_TABLE_NAME).append("(");
        sb.append(SequenceAttribute.DEFAULT_ID_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_VALUE_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_GMT_MODIFIED_COLUMN).append(") ");
        sb.append("VALUES(NULL, '%s', %s, NOW())");
        return String.format(sb.toString(), name, String.valueOf(start > 1 ? start : 0));
    }

    public String getSql(Type existingType) {
        String sql = null;
        switch (existingType) {
        case GROUP:
            sql = getSqlConvertedFromGroup();
            break;
        case SIMPLE:
            sql = getSqlConvertedFromSimple();
            break;
        case TIME:
            sql = getSqlConvertedFromTimeBased();
            break;
        default:
            sql = getSqlWithoutConversion();
            break;
        }
        return sql;
    }

    protected abstract String getSqlConvertedFromGroup();

    protected abstract String getSqlConvertedFromSimple();

    protected abstract String getSqlConvertedFromTimeBased();

    protected abstract String getSqlWithoutConversion();

}
