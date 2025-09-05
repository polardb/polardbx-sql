/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.util.PartTupleRouter;

import java.util.ArrayList;
import java.util.List;

public class ColumnarShardProcessor {
    /**
     * 列存分区算法。保留该方法，兼容旧版本，该接口只能适配一级分区
     *
     * @param partInfo 分区信息
     * @param values 一行数据，可能包含多个列
     * @param ec ec
     * @return 分区名称
     */
    public static String shard(PartitionInfo partInfo, List<Object> values, ExecutionContext ec) {
        return shard(partInfo, values, null, ec);
    }

    /**
     * 支持二级分区
     */
    public static String shard(PartitionInfo partInfo, List<Object> values, List<Object> subPartValues,
                               ExecutionContext ec) {
        PartTupleRouter router = new PartTupleRouter(partInfo, ec);
        router.init();

        /**
         * valuesOfAllLevelPartCols[0]: the values of 1st-level full part cols
         * valuesOfAllLevelPartCols[1]: the values of 2nd-level full part cols
         */
        List<List<Object>> valuesOfAllLevelPartCols = new ArrayList<>();
        valuesOfAllLevelPartCols.add(values);

        if (subPartValues != null && !subPartValues.isEmpty()) {
            valuesOfAllLevelPartCols.add(subPartValues);
        }

        PhysicalPartitionInfo phyPartInfo = router.routeTuple(valuesOfAllLevelPartCols);
        return phyPartInfo.getPartName();
    }

    /**
     * 由于旧版本PartTupleRouter初始化ExecutionContext.copy开销比较大，所以提供一个新方法，避免重复初始化，需注意ec的并发安全性.主要注意点为tmpEc.setParams(singleValueParams);
     * 如果useTmpCtx=true，则router.ec会被copy一份，能够并发安全
     * 如果useTmpCtx=false，则router.ec不会被copy，需调用线程保证并发安全
     */
    public static String shard(PartTupleRouter router, List<Object> values, List<Object> subPartValues,
                               boolean useTmpCtx) {

        /**
         * valuesOfAllLevelPartCols[0]: the values of 1st-level full part cols
         * valuesOfAllLevelPartCols[1]: the values of 2nd-level full part cols
         */
        List<List<Object>> valuesOfAllLevelPartCols = new ArrayList<>();
        valuesOfAllLevelPartCols.add(values);

        if (subPartValues != null && !subPartValues.isEmpty()) {
            valuesOfAllLevelPartCols.add(subPartValues);
        }

        PhysicalPartitionInfo phyPartInfo = router.routeTuple(valuesOfAllLevelPartCols, useTmpCtx);
        return phyPartInfo.getPartName();
    }
}
