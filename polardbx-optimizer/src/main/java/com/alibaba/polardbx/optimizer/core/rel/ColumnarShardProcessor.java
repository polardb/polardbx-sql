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
     * 新版列存分区算法，增加兼容性
     *
     * @param partInfo 分区信息
     * @param values 一行数据，可能包含多个列
     * @param ec ec
     * @return 分区名称
     */
    public static String shard(PartitionInfo partInfo, List<Object> values, ExecutionContext ec) {
        PartTupleRouter router = new PartTupleRouter(partInfo, ec);
        router.init();

        /**
         * valuesOfAllLevelPartCols[0]: the values of 1st-level full part cols
         * valuesOfAllLevelPartCols[1]: the values of 2nd-level full part cols
         */
        List<List<Object>> valuesOfAllLevelPartCols = new ArrayList<>();
        valuesOfAllLevelPartCols.add(values);
        PhysicalPartitionInfo phyPartInfo = router.routeTuple(valuesOfAllLevelPartCols);
        return phyPartInfo.getPartName();
    }
}
