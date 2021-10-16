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

import com.alibaba.polardbx.optimizer.core.sequence.ISequence;
import com.alibaba.polardbx.optimizer.core.sequence.sequence.ICreateSequence;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.CYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_MAX_VALUE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NOCYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_UNIT_INDEX;

/**
 * create sequence
 *
 * @author agapple 2014年12月18日 下午7:00:51
 * @since 5.1.17
 */
public abstract class CreateSequence extends Sequence<ICreateSequence> implements ICreateSequence {

    protected Type type = Type.GROUP;
    protected long start = DEFAULT_START_WITH;
    protected int increment = DEFAULT_INCREMENT_BY;
    protected long maxValue = DEFAULT_MAX_VALUE;
    protected int cycle = NOCYCLE;
    protected int unitCount = UNDEFINED_UNIT_COUNT;
    protected int unitIndex = UNDEFINED_UNIT_INDEX;
    protected int innerStep = UNDEFINED_INNER_STEP;

    public CreateSequence(String name) {
        this.name = name;
    }

    @Override
    public String toStringWithInden(int inden, ExplainResult.ExplainMode mode) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ").append(type.getKeyword());
        sb.append(" SEQUENCE ").append(name);
        if (start > 0) {
            sb.append(" START WITH ").append(start);
        }
        if (increment > 0) {
            sb.append(" INCREMENT BY ").append(increment);
        }
        if (maxValue > 0) {
            sb.append(" MAXVALUE ").append(maxValue);
        }
        if (cycle == NOCYCLE) {
            sb.append(" NOCYCLE");
        } else if (cycle == CYCLE) {
            sb.append(" CYCLE");
        }
        if (unitCount > 0 && unitIndex >= 0) {
            sb.append(" UNIT COUNT ").append(unitCount).append(" INDEX ").append(unitIndex);
        }
        if (innerStep > 0) {
            sb.append(" STEP ").append(innerStep);
        }
        return sb.toString();
    }

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
        if (start < 1 || start > Long.MAX_VALUE) {
            this.start = DEFAULT_START_WITH;
        } else {
            this.start = start;
        }
    }

    public int getIncrement() {
        return increment;
    }

    public void setIncrement(int increment) {
        if (increment < 1 || increment > Integer.MAX_VALUE) {
            this.increment = DEFAULT_INCREMENT_BY;
        } else {
            this.increment = increment;
        }
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        if (maxValue < 1 || maxValue > Long.MAX_VALUE) {
            this.maxValue = Long.MAX_VALUE;
        } else {
            this.maxValue = maxValue;
        }
    }

    public int getCycle() {
        return cycle;
    }

    public void setCycle(int cycle) {
        if (cycle == CYCLE) {
            this.cycle = CYCLE;
        } else {
            this.cycle = NOCYCLE;
        }
    }

    public int getUnitCount() {
        return unitCount;
    }

    public void setUnitCount(int unitCount) {
        this.unitCount = unitCount;
    }

    public int getUnitIndex() {
        return unitIndex;
    }

    public void setUnitIndex(int unitIndex) {
        this.unitIndex = unitIndex;
    }

    public int getInnerStep() {
        return innerStep;
    }

    public void setInnerStep(int innerStep) {
        this.innerStep = innerStep;
    }

    @Override
    public ISequence.SEQUENCE_DDL_TYPE getSequenceDdlType() {
        return ISequence.SEQUENCE_DDL_TYPE.CREATE_SEQUENCE;
    }

}
