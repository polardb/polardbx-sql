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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class Sequence {

    private Long start;
    private Integer increment;      // increment by
    private Long maxValue;
    private Integer unitCount;
    private Integer unitIndex;
    private SequenceAttribute.Type type;
    private SequenceAttribute.Type toType;
    private Integer innerStep;      // step
    private SqlKind kind;
    private Boolean cycle;
    private String sequenceName;
    private String newSequenceName;
    private String schemaName;

    public Sequence() {
    }

    private Sequence(AutoIncrementType type) {
        this.type = getType(type);
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

    public SequenceAttribute.Type getType() {
        return type;
    }

    public void setType(SequenceAttribute.Type type) {
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
        case GROUP:
            type = SequenceAttribute.Type.GROUP;
            break;
        case SIMPLE:
            type = SequenceAttribute.Type.SIMPLE;
            break;
        case TIME:
            type = SequenceAttribute.Type.TIME;
            break;
        default:
            type = SequenceAttribute.Type.NA;
            break;
        }
    }

    public SequenceAttribute.Type getToType() {
        return toType;
    }

    public void setToType(AutoIncrementType autoIncrementType) {
        switch (autoIncrementType) {
        case GROUP:
            toType = SequenceAttribute.Type.GROUP;
            break;
        case SIMPLE:
            toType = SequenceAttribute.Type.SIMPLE;
            break;
        case TIME:
            toType = SequenceAttribute.Type.TIME;
            break;

        default:
            throw new NotSupportException("Not supported change to " + autoIncrementType.name());
        }
    }

    public static SequenceAttribute.Type getType(AutoIncrementType autoIncrementType) {
        SequenceAttribute.Type type = SequenceAttribute.Type.NA;
        switch (autoIncrementType) {
        case GROUP:
            type = SequenceAttribute.Type.GROUP;
            break;
        case SIMPLE:
            type = SequenceAttribute.Type.SIMPLE;
            break;
        case TIME:
            type = SequenceAttribute.Type.TIME;
            break;
        default:
            type = SequenceAttribute.Type.NA;
            break;
        }
        return type;
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
        if (toType != null && toType != SequenceAttribute.Type.NA) {
            if (start == null && toType != SequenceAttribute.Type.TIME) {
                throw new NotSupportException(
                    "Not supported changing to " + toType.name() + " without assign START WITH!");
            }
        } else {
            sequenceBean.setToType(SequenceAttribute.Type.NA);
        }
        sequenceBean.setInnerStep(innerStep);//step
        sequenceBean.setKind(kind);
        sequenceBean.setCycle(cycle);
        sequenceBean.setSequenceName(SQLUtils.normalizeNoTrim(sequenceName));
        sequenceBean.setNewSequenceName(SQLUtils.normalizeNoTrim(newSequenceName));
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
