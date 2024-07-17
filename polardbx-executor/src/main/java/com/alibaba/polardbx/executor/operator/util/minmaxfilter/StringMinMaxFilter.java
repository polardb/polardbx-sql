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
import com.alibaba.polardbx.executor.chunk.SliceBlock;

/**
 * @author chenzilin
 */
public class StringMinMaxFilter extends MinMaxFilter {

    private String min;
    private String max;

    public StringMinMaxFilter() {

    }

    public StringMinMaxFilter(String min, String max) {
        this.min = min;
        this.max = max;
    }

    public String getMin() {
        return min;
    }

    public void setMin(String min) {
        this.min = min;
    }

    public String getMax() {
        return max;
    }

    public void setMax(String max) {
        this.max = max;
    }

    @Override
    public void put(Block block, int pos) {
        if (!block.isNull(pos)) {
            String str = (block.cast(SliceBlock.class)).getRegion(pos).toStringUtf8();
            if (min == null || str.compareTo(min) < 0) {
                min = str;
            }
            if (max == null || str.compareTo(max) > 0) {
                max = str;
            }
        }
    }

    @Override
    public MinMaxFilterInfo toMinMaxFilterInfo() {
        return new MinMaxFilterInfo(MinMaxFilterInfo.TYPE.STRING, null, null, min, max, null, null, null, null);
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
        return min;
    }

    @Override
    public String getMaxString() {
        return max;
    }
}

