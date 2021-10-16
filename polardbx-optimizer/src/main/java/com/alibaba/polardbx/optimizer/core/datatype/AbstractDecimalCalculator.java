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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.datatype.Decimal;

import java.math.BigDecimal;

/**
 * @author Eric Fu
 */
public abstract class AbstractDecimalCalculator extends AbstractCalculator {

    public abstract Decimal convertToDecimal(Object v);

    @Override
    public Object doAdd(Object v1, Object v2) {
        Decimal i1 = convertToDecimal(v1);
        Decimal i2 = convertToDecimal(v2);
        return i1.add(i2);
    }

    @Override
    public Object doSub(Object v1, Object v2) {
        Decimal i1 = convertToDecimal(v1);
        Decimal i2 = convertToDecimal(v2);
        return i1.subtract(i2);
    }

    @Override
    public Object doMultiply(Object v1, Object v2) {
        Decimal i1 = convertToDecimal(v1);
        Decimal i2 = convertToDecimal(v2);
        return i1.multiply(i2);
    }

    @Override
    public Object doDivide(Object v1, Object v2) {
        Decimal i1 = convertToDecimal(v1);
        Decimal i2 = convertToDecimal(v2);

        if (i2.compareTo(Decimal.ZERO) == 0) {
            return null;
        }

        try {
            return i1.divide(i2);
        } catch (ArithmeticException e) {
            // by zero
            if (e.getMessage().contains("by zero")) {
                // 0返回为null
                return null;
            } else {
                throw e;
            }
        }
    }

    @Override
    public Object doMod(Object v1, Object v2) {
        Decimal i1 = convertToDecimal(v1);
        Decimal i2 = convertToDecimal(v2);

        if (i2.equals(Decimal.ZERO)) {
            return null;
        }

        return i1.remainder(i2);
    }

    @Override
    public Object doAnd(Object v1, Object v2) {
        Decimal i1 = convertToDecimal(v1);
        Decimal i2 = convertToDecimal(v2);
        return (i1.compareTo(Decimal.ZERO) != 0) && (i2.compareTo(Decimal.ZERO) != 0);
    }

    @Override
    public Object doOr(Object v1, Object v2) {
        Decimal i1 = convertToDecimal(v1);
        Decimal i2 = convertToDecimal(v2);
        return (i1.compareTo(Decimal.ZERO) != 0) || (i2.compareTo(Decimal.ZERO) != 0);
    }

    @Override
    public Object doNot(Object v1) {
        Decimal i1 = convertToDecimal(v1);
        return i1.compareTo(Decimal.ZERO) == 0;
    }

    @Override
    public Object doBitAnd(Object v1, Object v2) {
        BigDecimal i1 = convertToDecimal(v1).toBigDecimal();
        BigDecimal i2 = convertToDecimal(v2).toBigDecimal();
        return i1.toBigInteger().and(i2.toBigInteger());
    }

    @Override
    public Object doBitOr(Object v1, Object v2) {
        BigDecimal i1 = convertToDecimal(v1).toBigDecimal();
        BigDecimal i2 = convertToDecimal(v2).toBigDecimal();
        return i1.toBigInteger().or(i2.toBigInteger());
    }

    @Override
    public Object doBitNot(Object v1) {
        BigDecimal i1 = convertToDecimal(v1).toBigDecimal();
        return i1.toBigInteger().not();
    }

    @Override
    public Object doXor(Object v1, Object v2) {
        Decimal i1 = convertToDecimal(v1);
        Decimal i2 = convertToDecimal(v2);
        return (i1.compareTo(Decimal.ZERO) != 0) ^ (i2.compareTo(Decimal.ZERO) != 0);
    }

    @Override
    public Object doBitXor(Object v1, Object v2) {
        BigDecimal i1 = convertToDecimal(v1).toBigDecimal();
        BigDecimal i2 = convertToDecimal(v2).toBigDecimal();
        return i1.toBigInteger().xor(i2.toBigInteger());
    }
}
