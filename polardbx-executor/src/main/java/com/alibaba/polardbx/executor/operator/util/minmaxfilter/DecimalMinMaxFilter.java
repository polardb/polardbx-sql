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

package com.alibaba.polardbx.executor.operator.util.minmaxfilter;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.bloomfilter.MinMaxFilterInfo;
import com.alibaba.polardbx.executor.chunk.Block;

/**
 * @author chenzilin
 */
public class DecimalMinMaxFilter extends MinMaxFilter {

    private Decimal min;
    private Decimal max;

    public DecimalMinMaxFilter() {

    }

    public DecimalMinMaxFilter(Decimal min, Decimal max) {
        this.min = min;
        this.max = max;
    }

    public Decimal getMin() {
        return min;
    }

    public void setMin(Decimal min) {
        this.min = min;
    }

    public Decimal getMax() {
        return max;
    }

    public void setMax(Decimal max) {
        this.max = max;
    }

    @Override
    public void put(Block block, int pos) {
        if (!block.isNull(pos)) {
            Decimal decimal = block.getDecimal(pos);
            if (min == null || decimal.compareTo(min) < 0) {
                min = decimal;
            }
            if (max == null || decimal.compareTo(max) > 0) {
                max = decimal;
            }
        }
    }

    @Override
    public MinMaxFilterInfo toMinMaxFilterInfo() {
        return new MinMaxFilterInfo(MinMaxFilterInfo.TYPE.DECIMAL, null, null, min == null ? null : min.toString(),
            max == null ? null : max.toString(), null,
            null, null, null);
    }

    @Override
    public Number getMinNumber() {
        return null;
    }

    @Override
    public Number getMaxNumber() {
        return null;
    }

    @Override
    public String getMinString() {
        return min.toString();
    }

    @Override
    public String getMaxString() {
        return max.toString();
    }
}


