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

package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;

import java.util.List;

/**
 * @see TaskHelper#fromDdlEngineTaskRecord(com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord)
 */
public interface CostEstimableDdlTask {

    void setCostInfo(CostInfo costInfo);

    CostInfo getCostInfo();

    class CostInfo {
        public final long rows;
        public final long dataSize;

        @JSONCreator
        private CostInfo(long rows, long dataSize) {
            this.rows = rows;
            this.dataSize = dataSize;
        }

        public static CostInfo combine(CostInfo c1, CostInfo c2) {
            long rows = 0L;
            long dataSize = 0L;
            if (c1 != null) {
                rows += c1.rows;
                dataSize += c1.dataSize;
            }
            if (c2 != null) {
                rows += c2.rows;
                dataSize += c2.dataSize;
            }
            return new CostInfo(rows, dataSize);
        }
    }

    static CostInfo createCostInfo(Long rows, Long dataSize) {
        return new CostInfo(rows != null ? rows : 0L, dataSize != null ? dataSize : 0L);
    }

    static CostInfo aggregate(List<CostInfo> costInfoList) {
        CostInfo result = new CostInfo(0, 0);
        if (costInfoList == null || costInfoList.size() == 0) {
            return result;
        }
        for (CostInfo c : costInfoList) {
            result = CostInfo.combine(result, c);
        }
        return result;
    }

}
