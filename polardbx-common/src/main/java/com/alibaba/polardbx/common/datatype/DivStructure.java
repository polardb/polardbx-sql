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

package com.alibaba.polardbx.common.datatype;


public class DivStructure {

    public static final long MAX_QUOTIENT = 1000000000000000000L;
    public static final long MIN_QUOTIENT = -1000000000000000000L;

    public static final int QUOTIENT_INDEX = 0;
    public static final int REMINDER_INDEX = 1;

    final long[] div;

    public DivStructure() {
        this.div = new long[2];
    }

    public static DivStructure fromDecimal(Decimal decimal) {
        DivStructure divStructure = new DivStructure();
        decimal.getDecimalStructure().toDiv(divStructure);
        return divStructure;
    }

    public static DivStructure fromDouble(Double value) {
        if (value == null || value > MAX_QUOTIENT || value < MIN_QUOTIENT) {
            return null;
        }

        long quot = (long) (value > 0 ? Math.floor(value) : Math.ceil(value));

        long rem = (long) ((value - quot) * 1000_000_000L);

        if (rem > 999999999L) {
            rem = 999999999L;
        } else if (rem < -999999999L) {
            rem = -999999999L;
        }

        DivStructure divStructure = new DivStructure();
        divStructure.setRem(rem);
        divStructure.setQuot(quot);
        return divStructure;
    }

    public long getRem() {
        return div[REMINDER_INDEX];
    }

    public long getQuot() {
        return div[QUOTIENT_INDEX];
    }

    public long[] getDiv() {
        return div;
    }

    public void setRem(long rem) {
        div[REMINDER_INDEX] = rem;
    }

    public void setQuot(long quot) {
        div[QUOTIENT_INDEX] = quot;
    }
}