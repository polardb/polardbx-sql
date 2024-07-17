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

import com.alibaba.polardbx.common.utils.bloomfilter.MinMaxFilterInfo;
import com.alibaba.polardbx.executor.chunk.Block;

/**
 * @author chenzilin
 */
public class DoubleMinMaxFilter extends MinMaxFilter {
    Double min;
    Double max;

    public DoubleMinMaxFilter() {

    }

    public DoubleMinMaxFilter(Double min, Double max) {
        this.min = min;
        this.max = max;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    @Override
    public void put(Block block, int pos) {
        if (!block.isNull(pos)) {
            double num = block.getDouble(pos);
            if (min == null || num < min) {
                min = num;
            }
            if (max == null || num > max) {
                max = num;
            }
        }
    }

    @Override
    public MinMaxFilterInfo toMinMaxFilterInfo() {
        return new MinMaxFilterInfo(
            MinMaxFilterInfo.TYPE.DOUBLE,
            null, null, null, null, min == null ? null : min.doubleValue(),
            max == null ? null : max.doubleValue(), null, null);
    }

    @Override
    public Number getMinNumber() {
        return min;
    }

    @Override
    public Number getMaxNumber() {
        return max;
    }

    @Override
    public String getMinString() {
        return null;
    }

    @Override
    public String getMaxString() {
        return null;
    }
}

