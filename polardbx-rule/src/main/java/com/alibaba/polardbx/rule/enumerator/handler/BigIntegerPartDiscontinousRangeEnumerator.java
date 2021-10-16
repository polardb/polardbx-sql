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

import java.math.BigInteger;

public class BigIntegerPartDiscontinousRangeEnumerator extends NumberPartDiscontinousRangeEnumerator {

    @Override
    protected BigInteger cast2Number(Comparable begin) {
        return (BigInteger) begin;
    }

    @Override
    protected BigInteger getNumber(Comparable begin) {
        return (BigInteger) begin;
    }

    @Override
    protected BigInteger plus(Number begin, int plus) {
        return ((BigInteger) begin).add(BigInteger.valueOf(plus));
    }

    protected boolean inputCloseRangeGreaterThanMaxFieldOfDifination(Comparable from, Comparable to,
                                                                     Integer cumulativeTimes,
                                                                     Comparable<?> atomIncrValue) {
        if (cumulativeTimes == null) {
            return false;
        }
        if (atomIncrValue == null) {
            atomIncrValue = DEFAULT_LONG_ATOMIC_VALUE;
        }
        BigInteger fromBig = (BigInteger) cast2Number(from);
        BigInteger toBig = (BigInteger) cast2Number(to);
        int atomIncValLong = ((Number) atomIncrValue).intValue();
        int size = cumulativeTimes;
        if ((toBig.subtract(fromBig).compareTo(BigInteger.valueOf(atomIncValLong * size)) > 0)) {
            return true;
        } else {
            return false;
        }
    }
}
