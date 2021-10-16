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

package com.alibaba.polardbx.rule.enumerator.handler;

import java.util.Set;

import com.alibaba.polardbx.common.model.sqljep.Comparative;

public abstract class NumberPartDiscontinousRangeEnumerator extends PartDiscontinousRangeEnumerator {

    protected static final int LIMIT_UNIT_OF_LONG = 1;
    protected static final int DEFAULT_LONG_ATOMIC_VALUE = 1;

    @Override
    protected Comparative changeGreater2GreaterOrEq(Comparative from) {
        if (from.getComparison() == Comparative.GreaterThan) {
            Number fromComparable = cast2Number((Comparable) from.getValue());
            return new Comparative(Comparative.GreaterThanOrEqual,
                (Comparable) plus(fromComparable, LIMIT_UNIT_OF_LONG));
        } else {
            return from;
        }
    }

    @Override
    protected Comparative changeLess2LessOrEq(Comparative to) {
        if (to.getComparison() == Comparative.LessThan) {
            Number toComparable = cast2Number((Comparable) to.getValue());
            return new Comparative(Comparative.LessThanOrEqual,
                (Comparable) plus(toComparable, -1 * LIMIT_UNIT_OF_LONG));
        } else {
            return to;
        }
    }

    @Override
    protected Comparable getOneStep(Comparable source, Comparable atomIncVal) {
        if (atomIncVal == null) {
            atomIncVal = DEFAULT_LONG_ATOMIC_VALUE;
        }
        Number sourceLong = cast2Number(source);
        int atomIncValInt = (Integer) atomIncVal;
        return (Comparable) plus(sourceLong, atomIncValInt);
    }

    @SuppressWarnings("rawtypes")
    protected boolean inputCloseRangeGreaterThanMaxFieldOfDifination(Comparable from, Comparable to,
                                                                     Integer cumulativeTimes,
                                                                     Comparable<?> atomIncrValue) {
        if (cumulativeTimes == null) {
            return false;
        }
        if (atomIncrValue == null) {
            atomIncrValue = DEFAULT_LONG_ATOMIC_VALUE;
        }
        long fromLong = ((Number) from).longValue();
        long toLong = ((Number) to).longValue();
        int atomIncValLong = ((Number) atomIncrValue).intValue();
        int size = cumulativeTimes;
        if ((toLong - fromLong) > (atomIncValLong * size)) {
            return true;
        } else {
            return false;
        }
    }

    public void processAllPassableFields(Comparative begin, Set<Object> retValue, Integer cumulativeTimes,
                                         Comparable<?> atomicIncreationValue) {
        if (cumulativeTimes == null) {
            throw new IllegalStateException("在没有提供叠加次数的前提下，不能够根据当前范围条件选出对应的定义域的枚举值，sql中不要出现> < >= <=");
        }
        if (atomicIncreationValue == null) {
            atomicIncreationValue = DEFAULT_LONG_ATOMIC_VALUE;
        }
        // 把> < 替换为>= <=
        begin = changeGreater2GreaterOrEq(begin);
        begin = changeLess2LessOrEq(begin);

        // long beginInt = (Long) toPrimaryValue(begin.getValue());
        Number beginInt = getNumber((Comparable) begin.getValue());
        int atomicIncreateValueInt = ((Number) atomicIncreationValue).intValue();
        int comparasion = begin.getComparison();

        if (comparasion == Comparative.GreaterThanOrEqual) {
            for (int i = 0; i < cumulativeTimes; i++) {
                retValue.add(plus(beginInt, atomicIncreateValueInt * i));
            }
        } else if (comparasion == Comparative.LessThanOrEqual) {
            for (int i = 0; i < cumulativeTimes; i++) {
                //支持对负数的处理
                Number value = (Number) plus(beginInt, -1 * atomicIncreateValueInt * i);
                retValue.add(value);
            }
        } else if (comparasion == Comparative.NotEquivalent) {
            // not equivalent operator needs full sharding table scan.
            for (int i = 0; i < cumulativeTimes; i++) {
                retValue.add(plus(beginInt, atomicIncreateValueInt * i));
                retValue.add(plus(beginInt, -1 * atomicIncreateValueInt * i));
            }
        }
    }

    protected abstract Number cast2Number(Comparable begin);

    protected abstract Number getNumber(Comparable begin);

    protected abstract Number plus(Number begin, int plus);
}
