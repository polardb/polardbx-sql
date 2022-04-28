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
 * @date 2021/12/14 18:20
 */
public class FloatMinMaxFilter extends MinMaxFilter {
    Float min;
    Float max;
    public FloatMinMaxFilter() {

    }

    public FloatMinMaxFilter(Float min, Float max) {
        this.min = min;
        this.max = max;
    }

    public Float getMin() {
        return min;
    }

    public void setMin(Float min) {
        this.min = min;
    }

    public Float getMax() {
        return max;
    }

    public void setMax(Float max) {
        this.max = max;
    }

    @Override
    public void put(Block block, int pos) {
        if (!block.isNull(pos)) {
            float num = block.getFloat(pos);
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
                MinMaxFilterInfo.TYPE.FLOAT,
                null, null, null, null, null, null, min == null ? null : min.floatValue(),
                max == null ? null : max.floatValue());
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


