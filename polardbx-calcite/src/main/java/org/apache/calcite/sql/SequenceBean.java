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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class SequenceBean {
    private Long start;
    private Integer increment;
    private Long maxValue;
    private Integer unitCount;
    private Integer unitIndex;
    private SequenceAttribute.Type type;
    private SequenceAttribute.Type toType;
    private Integer innerStep;
    private SqlKind kind;
    private Boolean cycle;
    private boolean isNew;
    private String sequenceName;
    private String newSequenceName;
    private String schemaName;

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

    public Integer getUnitIndex() {
        return unitIndex;
    }

    public void setUnitIndex(Integer unitIndex) {
        this.unitIndex = unitIndex;
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
        this.type = convertAutoIncrementType(autoIncrementType);
    }

    public static Type convertAutoIncrementType(AutoIncrementType autoIncrementType) {
        if (autoIncrementType == null) {
            return Type.NA;
        }

        Type type;
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

    public SequenceAttribute.Type getToType() {
        return toType;
    }

    public void setToType(SequenceAttribute.Type toType) {
        this.toType = toType;
    }

    public SqlKind getKind() {
        return kind;
    }

    public void setKind(SqlKind kind) {
        this.kind = kind;
    }

    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean aNew) {
        isNew = aNew;
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

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
