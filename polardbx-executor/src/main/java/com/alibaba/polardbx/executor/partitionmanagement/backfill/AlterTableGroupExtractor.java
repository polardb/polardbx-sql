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

package com.alibaba.polardbx.executor.partitionmanagement.backfill;

import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import org.apache.calcite.sql.SqlSelect;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupExtractor extends Extractor {
    final Map<String, Set<String>> sourcePhyTables;

    protected AlterTableGroupExtractor(String schemaName, String sourceTableName, String targetTableName,
                                       long batchSize,
                                       long speedMin,
                                       long speedLimit,
                                       long parallelism,
                                       boolean useBinary,
                                       PhyTableOperation planSelectWithMax,
                                       PhyTableOperation planSelectWithMin,
                                       PhyTableOperation planSelectWithMinAndMax,
                                       PhyTableOperation planSelectMaxPk,
                                       PhyTableOperation planSelectSample,
                                       List<Integer> primaryKeysId,
                                       Map<String, Set<String>> sourcePhyTables) {
        super(schemaName, sourceTableName, targetTableName, batchSize, speedMin, speedLimit, parallelism, useBinary,
            null, planSelectWithMax, planSelectWithMin, planSelectWithMinAndMax, planSelectMaxPk,
            planSelectSample, primaryKeysId);
        this.sourcePhyTables = sourcePhyTables;
    }

    public static Extractor create(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                                   long speedMin, long speedLimit, long parallelism, boolean useBinary,
                                   Map<String, Set<String>> sourcePhyTables,
                                   ExecutionContext ec) {
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, useBinary, ec);

        ExtractorInfo info = Extractor.buildExtractorInfo(ec, schemaName, sourceTableName, targetTableName, true);

        SqlSelect.LockMode lockMode = SqlSelect.LockMode.SHARED_LOCK;

        return new AlterTableGroupExtractor(schemaName,
            sourceTableName,
            targetTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, true, lockMode),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, false,
                lockMode),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, true,
                lockMode),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys()),
            info.getPrimaryKeysId(),
            sourcePhyTables);
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        return sourcePhyTables;
    }
}
