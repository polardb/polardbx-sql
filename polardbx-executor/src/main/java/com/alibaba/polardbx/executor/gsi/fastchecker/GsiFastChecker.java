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
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GsiFastChecker extends FastChecker {
    public GsiFastChecker(String schemaName, String srcLogicalTableName, String dstLogicalTableName,
                          Map<String, Set<String>> srcPhyDbAndTables, Map<String, Set<String>> dstPhyDbAndTables,
                          List<String> srcColumns, List<String> dstColumns, PhyTableOperation planSelectHashCheckSrc,
                          PhyTableOperation planSelectHashCheckDst,
                          PhyTableOperation planIdleSelectSrc, PhyTableOperation planIdleSelectDst,
                          long parallelism, int lockTimeOut) {
        super(schemaName, srcLogicalTableName, dstLogicalTableName, null, srcPhyDbAndTables, dstPhyDbAndTables,
            srcColumns, dstColumns, planSelectHashCheckSrc, planSelectHashCheckDst, planIdleSelectSrc,
            planIdleSelectDst, parallelism, lockTimeOut);
    }

    public static FastChecker create(String schemaName, String tableName, String indexName, long parallelism,
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

        final List<String> indexColumns = indexTableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());
        final List<String> baseTableColumns = new ArrayList<>(indexColumns);

        final Map<String, Set<String>> srcPhyDbAndTables = GsiUtils.getPhyTables(schemaName, tableName);
        final Map<String, Set<String>> dstPhyDbAndTables = GsiUtils.getPhyTables(schemaName, indexName);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        final int lockTimeOut = ec.getParamManager().getInt(ConnectionParams.FASTCHECKER_LOCK_TIMEOUT);

        return new GsiFastChecker(schemaName, tableName, indexName,
            srcPhyDbAndTables, dstPhyDbAndTables,
            baseTableColumns, indexColumns,
            builder.buildSelectHashCheckForChecker(baseTableMeta, baseTableColumns),
            builder.buildSelectHashCheckForChecker(indexTableMeta, indexColumns),
            builder.buildIdleSelectForChecker(baseTableMeta, baseTableColumns),
            builder.buildIdleSelectForChecker(indexTableMeta, indexColumns),
            parallelism, lockTimeOut);
    }

    /**
     * In FastChecker, we use tsoCheck and xaCheckForIsomorphicTable to exec check.
     * In GsiFastChecker, we use tsoCheck and xaCheckForHeterogeneousTable,
     * for GSI tables are heterogeneous.
     */
    @Override
    public boolean check(ExecutionContext baseEc) {
        boolean tsoCheckResult = tsoCheck(baseEc);
        if (tsoCheckResult) {
            return true;
        } else {
            SQLRecorderLogger.ddlLogger
                .warn(MessageFormat.format("[{0}] FastChecker with TsoCheck failed, begin XaCheck",
                    baseEc.getTraceId()));
        }

        /**
         * When tsoCheck is failed, bypath to use old checker directly.
         * because xaCheck of gsi is easily to caused deadlock by using lock tables
         */
        //boolean xaCheckResult = xaCheckForHeterogeneousTable(baseEc);
        return tsoCheckResult;
    }
}
