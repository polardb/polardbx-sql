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

package com.alibaba.polardbx.executor.physicalbackfill;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PhysicalBackfillExecutor {

    public PhysicalBackfillExecutor() {
    }

    public int backfill(String schemaName,
                        String tableName,
                        Map<String, Set<String>> sourcePhyTables,
                        Map<String, Set<String>> targetPhyTables,
                        Map<String, String> sourceTargetGroup,
                        boolean isBroadcast,
                        ExecutionContext ec) {
        final long batchSize = ec.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_BATCH_SIZE);
        final long minUpdateBatch =
            ec.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_MIN_SUCCESS_BATCH_UPDATE);
        final long parallelism = ec.getParamManager().getLong(ConnectionParams.PHYSICAL_BACKFILL_PARALLELISM);

        if (null == ec.getServerVariables()) {
            ec.setServerVariables(new HashMap<>());
        }
        PhysicalBackfillExtractor extractor =
            new PhysicalBackfillExtractor(schemaName, tableName, sourcePhyTables, targetPhyTables, sourceTargetGroup,
                isBroadcast,
                batchSize,
                parallelism,
                minUpdateBatch);
        physicalBackfillLoader loader = new physicalBackfillLoader(schemaName, tableName);
        extractor.doExtract(ec, new BatchConsumer() {
            @Override
            public void consume(Pair<String, String> targetDbAndGroup,
                                Pair<String, String> targetFileAndDir,
                                List<Pair<String, Integer>> targetHosts,
                                Pair<String, String> userInfo,
                                PolarxPhysicalBackfill.TransferFileDataOperator transferFileData) {
                loader.applyBatch(targetDbAndGroup, targetFileAndDir, targetHosts, userInfo, transferFileData, ec);
            }
        });
        return 0;
    }

}
