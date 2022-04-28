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
 * @date 2021/12/8 15:01
 */
public class BlackHoleMinMaxFilter extends MinMaxFilter {

    public BlackHoleMinMaxFilter() {

    }

    @Override
    public void put(Block block, int pos) {
        // eat it
    }

    @Override
    public MinMaxFilterInfo toMinMaxFilterInfo() {
        return new MinMaxFilterInfo(MinMaxFilterInfo.TYPE.NULL, null, null, null, null, null, null, null, null);
    }
}
