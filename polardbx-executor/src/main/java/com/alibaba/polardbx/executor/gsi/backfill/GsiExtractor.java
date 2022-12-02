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

package com.alibaba.polardbx.executor.gsi.backfill;

import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import org.apache.calcite.sql.SqlSelect;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lock, Read, Feed to consumer
 */
public class GsiExtractor extends Extractor {

    public GsiExtractor(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                        long speedMin,
                        long speedLimit,
                        long parallelism, PhyTableOperation planSelectWithMax,
                        PhyTableOperation planSelectWithMin,
                        PhyTableOperation planSelectWithMinAndMax,
                        PhyTableOperation planSelectMaxPk,
                        PhyTableOperation planSelectSample,
                        PhyTableOperation planSelectMinAndMaxSample,
                        List<Integer> primaryKeysId) {
        super(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit, parallelism,
            planSelectWithMax, planSelectWithMin, planSelectWithMinAndMax, planSelectMaxPk,
            planSelectSample, planSelectMinAndMaxSample, primaryKeysId);
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        return GsiUtils.getPhyTables(schemaName, sourceTableName);
    }

    public static Extractor create(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                                   long speedMin, long speedLimit, long parallelism, ExecutionContext ec) {
        ExtractorInfo info = Extractor.buildExtractorInfo(ec, schemaName, sourceTableName, targetTableName);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new GsiExtractor(schemaName,
            sourceTableName,
            targetTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, true, SqlSelect.LockMode.SHARED_LOCK),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, false,
                SqlSelect.LockMode.SHARED_LOCK),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, true,
                SqlSelect.LockMode.SHARED_LOCK),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys(), info.getPrimaryKeys(),
                false, false),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys(), info.getPrimaryKeys(),
                true, true),
            info.getPrimaryKeysId());
    }
}
