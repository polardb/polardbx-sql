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

/**
 * @author chensr 2016年12月2日 上午11:49:48
 * @since 5.0.0
 */
public abstract class AlterSequenceOpt extends AlterSequence {

    public AlterSequenceOpt(String schemaName, String name) {
        super(schemaName, name);
    }

    protected String getSqlCreateSimple(boolean withCache) {
        return getSqlAssembleSimple(getSqlCreateBase(), withCache);
    }

    protected String getSqlCreateTimeBased() {
        return getSqlAssembleTimeBased(getSqlCreateBase());
    }

    protected String getSqlDropGroup() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(SequenceAttribute.DEFAULT_GROUP_TABLE_NAME);
        sb.append(" WHERE ").append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(" = '").append(name).append("'");
        return sb.toString();
    }

    protected String getSqlAlterSimple(boolean toCache) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(SequenceAttribute.DEFAULT_TABLE_NAME).append(" SET ");
        sb.append(SequenceAttribute.DEFAULT_START_WITH_COLUMN).append(" = ");
        if (start > 0) {
            sb.append(start).append(", ");
            // Change current value if user specifies a new initial value.
            sb.append(SequenceAttribute.DEFAULT_VALUE_COLUMN).append(" = ").append(start);
        } else {
            sb.append(SequenceAttribute.DEFAULT_START_WITH_COLUMN);
        }
        if (increment > 0) {
            // We should first change current value to capture the new increment
            // if start with is not specified,
            if (start <= 0) {
                sb.append(", ").append(SequenceAttribute.DEFAULT_VALUE_COLUMN).append(" = ");
                sb.append(SequenceAttribute.DEFAULT_VALUE_COLUMN).append(" - ");
                sb.append(SequenceAttribute.DEFAULT_INCREMENT_BY_COLUMN).append(" + ").append(increment);
            }
            // then update increment itself.
            sb.append(", ").append(SequenceAttribute.DEFAULT_INCREMENT_BY_COLUMN).append(" = ").append(increment);
        }
        if (maxValue > 0) {
            sb.append(", ").append(SequenceAttribute.DEFAULT_MAX_VALUE_COLUMN).append(" = ").append(maxValue);
        }
        // We have to carefully handle cycle and cache flags.
        sb.append(", ").append(SequenceAttribute.DEFAULT_CYCLE_COLUMN).append(" = (");
        sb.append(SequenceAttribute.DEFAULT_CYCLE_COLUMN);
        if (cycle == SequenceAttribute.CYCLE) {
            sb.append(" | ").append(cycle);
        } else if (cycle == SequenceAttribute.NOCYCLE) {
            sb.append(" & ").append(cycle | SequenceAttribute.CACHE_ENABLED);
        } else {
            sb.append(" | ").append(SequenceAttribute.DEFAULT_CYCLE_COLUMN);
        }
        sb.append(") ");
        if (toCache) {
            sb.append(" | ").append(SequenceAttribute.CACHE_ENABLED);
        } else {
            sb.append(" & ").append(SequenceAttribute.CACHE_DISABLED | SequenceAttribute.CYCLE);
        }
        sb.append(" WHERE ").append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(" = '").append(name).append("'");
        return sb.toString();
    }

    protected String getSqlAlterFromTimeBased(boolean withCache) {
        return getSqlAssembleSimple(getSqlAlterBase(), withCache);
    }

    protected String getSqlAlterToTimeBased() {
        return getSqlAssembleTimeBased(getSqlAlterBase());
    }

    private String getSqlCreateBase() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(SequenceAttribute.DEFAULT_TABLE_NAME).append("(");
        sb.append(SequenceAttribute.DEFAULT_VALUE_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_INCREMENT_BY_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_START_WITH_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_MAX_VALUE_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_CYCLE_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_GMT_MODIFIED_COLUMN).append(") ");
        sb.append("VALUES(%s, %s, %s, %s, %s, '%s', NOW())");
        return sb.toString();
    }

    private String getSqlAlterBase() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(SequenceAttribute.DEFAULT_TABLE_NAME).append(" SET ");
        sb.append(SequenceAttribute.DEFAULT_VALUE_COLUMN).append(" = %s, ");
        sb.append(SequenceAttribute.DEFAULT_INCREMENT_BY_COLUMN).append(" = %s, ");
        sb.append(SequenceAttribute.DEFAULT_START_WITH_COLUMN).append(" = %s, ");
        sb.append(SequenceAttribute.DEFAULT_MAX_VALUE_COLUMN).append(" = %s, ");
        sb.append(SequenceAttribute.DEFAULT_CYCLE_COLUMN).append(" = %s, ");
        sb.append(SequenceAttribute.DEFAULT_GMT_MODIFIED_COLUMN).append(" = NOW()");
        sb.append(" WHERE ").append(SequenceAttribute.DEFAULT_NAME_COLUMN).append(" = '%s'");
        return sb.toString();
    }

    private String getSqlAssembleSimple(String sql, boolean withCache) {
        if (cycle == SequenceAttribute.NA) {
            cycle = SequenceAttribute.NOCYCLE;
        }
        return String.format(sql,
            String.valueOf(start > 0 ? start : SequenceAttribute.DEFAULT_START_WITH),
            String.valueOf(increment > 0 ? increment : SequenceAttribute.DEFAULT_INCREMENT_BY),
            String.valueOf(start > 0 ? start : SequenceAttribute.DEFAULT_START_WITH),
            String.valueOf(maxValue > 0 ? maxValue : SequenceAttribute.DEFAULT_MAX_VALUE),
            String.valueOf(withCache ? cycle | SequenceAttribute.CACHE_ENABLED : cycle),
            name);
    }

    private String getSqlAssembleTimeBased(String sql) {
        return String.format(sql,
            String.valueOf(0),
            String.valueOf(0),
            String.valueOf(0),
            String.valueOf(0),
            String.valueOf(SequenceAttribute.TIME_BASED),
            name);
    }

}
