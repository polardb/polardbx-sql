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

package com.alibaba.polardbx.optimizer.parse.bean;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

public class Sequence {

    private Long start;
    private Integer increment;
    private Long maxValue;
    private Integer unitCount;
    private Integer unitIndex;
    private Type type;
    private Type toType;
    private Integer innerStep;
    private SqlKind kind;
    private Boolean cycle;
    private String sequenceName;
    private String newSequenceName;
    private String schemaName;

    public Sequence() {
    }

    private Sequence(AutoIncrementType type) {
        setType(type);
    }

    public static Sequence newSequence(AutoIncrementType type) {
        return new Sequence(type);
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Integer getIncrement() {
        return increment;
    }

    public void setIncrement(Integer increment) {
        this.increment = increment;
    }

    public Long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(Long maxValue) {
        this.maxValue = maxValue;
    }

    public Integer getUnitCount() {
        return unitCount;
    }

    public void setUnitCount(Integer unitCount) {
        this.unitCount = unitCount;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Integer getInnerStep() {
        return innerStep;
    }

    public void setInnerStep(Integer innerStep) {
        this.innerStep = innerStep;
    }

    public void setType(AutoIncrementType autoIncrementType) {
        switch (autoIncrementType) {
        case NEW:
            type = Type.NEW;
            break;
        case GROUP:
            type = Type.GROUP;
            break;
        case SIMPLE:
            type = Type.SIMPLE;
            break;
        case TIME:
            type = Type.TIME;
            break;
        default:
            type = Type.NA;
            break;
        }
    }

    public Type getToType() {
        return toType;
    }

    public void setToType(AutoIncrementType autoIncrementType) {
        switch (autoIncrementType) {
        case NEW:
            toType = Type.NEW;
            break;
        case GROUP:
            toType = Type.GROUP;
            break;
        case SIMPLE:
            toType = Type.SIMPLE;
            break;
        case TIME:
            toType = Type.TIME;
            break;
        default:
            throw new NotSupportException("Not supported change to " + autoIncrementType.name());
        }
    }

    public SqlKind getKind() {
        return kind;
    }

    public void setKind(SqlKind kind) {
        this.kind = kind;
    }

    public Boolean getCycle() {
        return cycle;
    }

    public void setCycle(Boolean cycle) {
        this.cycle = cycle;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public String getNewSequenceName() {
        return newSequenceName;
    }

    public void setNewSequenceName(String newSequenceName) {
        this.newSequenceName = newSequenceName;
    }

    public Integer getUnitIndex() {
        return unitIndex;
    }

    public void setUnitIndex(Integer unitIndex) {
        this.unitIndex = unitIndex;
    }

    public SequenceBean convertSequenceBeanInstance() {
        final SequenceBean sequenceBean = new SequenceBean();
        sequenceBean.setStart(start);
        numericValueExpected(start, "START WITH");
        sequenceBean.setIncrement(increment); //increment by
        numericValueExpected(increment, "INCREMENT BY");
        sequenceBean.setMaxValue(maxValue);
        numericValueExpected(maxValue, "MAXVALUE");
        sequenceBean.setUnitCount(unitCount);
        sequenceBean.setUnitIndex(unitIndex);
        sequenceBean.setType(type);
        sequenceBean.setToType(toType);
        if (toType != null && toType != Type.NA) {
            if (start == null && toType != Type.TIME) {
                throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE,
                    "Changing to " + toType.name() + " sequence must assign a START WITH value");
            }
        } else {
            sequenceBean.setToType(Type.NA);
        }
        sequenceBean.setInnerStep(innerStep);
        sequenceBean.setKind(kind);
        sequenceBean.setCycle(cycle);
        sequenceBean.setName(SQLUtils.normalizeNoTrim(sequenceName));
        sequenceBean.setNewName(SQLUtils.normalizeNoTrim(newSequenceName));
        sequenceBean.setSchemaName(schemaName);
        return sequenceBean;
    }

    private void numericValueExpected(Number number, String key) {
        if (number != null) {
            if (number.longValue() < 0) {
                throw new RuntimeException("Numeric value expected right after " + key + ",must be a positive number!");
            }
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
