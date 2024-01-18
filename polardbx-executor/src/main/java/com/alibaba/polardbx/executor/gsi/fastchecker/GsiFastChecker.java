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

package com.alibaba.polardbx.executor.gsi.fastchecker;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.commons.collections.MapUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GsiFastChecker extends FastChecker {
    public GsiFastChecker(String schemaName, String srcLogicalTableName, String dstLogicalTableName,
                          Map<String, Set<String>> srcPhyDbAndTables, Map<String, Set<String>> dstPhyDbAndTables,
                          List<String> srcColumns, List<String> dstColumns, List<String> srcPks, List<String> dstPks,
                          long parallelism, int lockTimeOut, PhyTableOperation planSelectHashCheckSrc,
                          PhyTableOperation planSelectHashCheckWithUpperBoundSrc,
                          PhyTableOperation planSelectHashCheckWithLowerBoundSrc,
                          PhyTableOperation planSelectHashCheckWithLowerUpperBoundSrc,
                          PhyTableOperation planSelectHashCheckDst,
                          PhyTableOperation planSelectHashCheckWithUpperBoundDst,
                          PhyTableOperation planSelectHashCheckWithLowerBoundDst,
                          PhyTableOperation planSelectHashCheckWithLowerUpperBoundDst,
                          PhyTableOperation planIdleSelectSrc, PhyTableOperation planIdleSelectDst,
                          PhyTableOperation planSelectSampleSrc, PhyTableOperation planSelectSampleDst) {
        super(schemaName, schemaName, srcLogicalTableName, dstLogicalTableName, null, srcPhyDbAndTables,
            dstPhyDbAndTables, srcColumns, dstColumns, srcPks, dstPks, parallelism, lockTimeOut, planSelectHashCheckSrc,
            planSelectHashCheckWithUpperBoundSrc, planSelectHashCheckWithLowerBoundSrc,
            planSelectHashCheckWithLowerUpperBoundSrc, planSelectHashCheckDst, planSelectHashCheckWithUpperBoundDst,
            planSelectHashCheckWithLowerBoundDst, planSelectHashCheckWithLowerUpperBoundDst, planIdleSelectSrc,
            planIdleSelectDst, planSelectSampleSrc, planSelectSampleDst);
    }

    public static FastChecker create(String schemaName, String tableName, String indexName,
                                     Map<String, String> virtualColumnMap, long parallelism,
                                     ExecutionContext ec) {
        // Build select plan
        final SchemaManager sm = ec.getSchemaManager(schemaName);
        final TableMeta indexTableMeta = sm.getTable(indexName);
        if (null == indexTableMeta || !indexTableMeta.isGsi()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER, "Incorrect GSI table.");
        }

        if (null == tableName) {
            tableName = indexTableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        }
        final TableMeta baseTableMeta = sm.getTable(tableName);

        if (null == baseTableMeta || !baseTableMeta.withGsi() || !indexTableMeta.isGsi()
            || !baseTableMeta.getGsiTableMetaBean().indexMap.containsKey(indexName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER, "Incorrect GSI relationship.");
        }

        final List<String> indexColumns =
            indexTableMeta.getAllColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
        final List<String> baseTableColumns = new ArrayList<>(indexColumns);

        // 重要：构造planSelectSampleSrc 和 planSelectSampleDst时，传入的主键必须按原本的主键顺序!
        final List<String> baseTablePks = FastChecker.getorderedPrimaryKeys(baseTableMeta, ec);
        final List<String> indexTablePks = FastChecker.getorderedPrimaryKeys(indexTableMeta, ec);

        final Map<String, Set<String>> srcPhyDbAndTables = GsiUtils.getPhyTables(schemaName, tableName);
        final Map<String, Set<String>> dstPhyDbAndTables = GsiUtils.getPhyTables(schemaName, indexName);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        final int lockTimeOut = ec.getParamManager().getInt(ConnectionParams.FASTCHECKER_LOCK_TIMEOUT);

        return new GsiFastChecker(schemaName, tableName, indexName, srcPhyDbAndTables, dstPhyDbAndTables,
            baseTableColumns, indexColumns, baseTablePks, indexTablePks, parallelism, lockTimeOut,
            builder.buildSelectHashCheckForGSIChecker(baseTableMeta, baseTableColumns, virtualColumnMap, baseTablePks,
                false, false),
            builder.buildSelectHashCheckForGSIChecker(baseTableMeta, baseTableColumns, virtualColumnMap, baseTablePks,
                false, true),
            builder.buildSelectHashCheckForGSIChecker(baseTableMeta, baseTableColumns, virtualColumnMap, baseTablePks,
                true, false),
            builder.buildSelectHashCheckForGSIChecker(baseTableMeta, baseTableColumns, virtualColumnMap, baseTablePks,
                true, true),

            builder.buildSelectHashCheckForChecker(indexTableMeta, indexColumns, indexTablePks, false, false),
            builder.buildSelectHashCheckForChecker(indexTableMeta, indexColumns, indexTablePks, false, true),
            builder.buildSelectHashCheckForChecker(indexTableMeta, indexColumns, indexTablePks, true, false),
            builder.buildSelectHashCheckForChecker(indexTableMeta, indexColumns, indexTablePks, true, true),

            builder.buildIdleSelectForChecker(baseTableMeta, baseTableColumns),
            builder.buildIdleSelectForChecker(indexTableMeta, indexColumns),

            builder.buildSqlSelectForSample(baseTableMeta, baseTablePks, baseTablePks, false, false),
            builder.buildSqlSelectForSample(indexTableMeta, indexTablePks, indexTablePks, false, false));
    }
}
