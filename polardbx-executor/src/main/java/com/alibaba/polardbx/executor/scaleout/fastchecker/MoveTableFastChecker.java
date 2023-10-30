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

package com.alibaba.polardbx.executor.scaleout.fastchecker;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveTableFastChecker extends FastChecker {
    public MoveTableFastChecker(String schemaName, String srcLogicalTableName, String dstLogicalTableName,
                                Map<String, Set<String>> srcPhyDbAndTables, Map<String, Set<String>> dstPhyDbAndTables,
                                List<String> srcColumns, List<String> dstColumns, List<String> srcPks,
                                List<String> dstPks, long parallelism, int lockTimeOut,
                                PhyTableOperation planSelectHashCheckSrc,
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

    public static FastChecker create(String schemaName, String tableName, Map<String, String> sourceTargetGroup,
                                     Map<String, Set<String>> srcPhyDbAndTables,
                                     Map<String, Set<String>> dstPhyDbAndTables, long parallelism,
                                     ExecutionContext ec) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta tableMeta = sm.getTable(tableName);

        if (null == tableMeta) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, "Incorrect SCALEOUT relationship.");
        }

        final List<String> allColumns =
            tableMeta.getAllColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());

        // 重要：构造planSelectSampleSrc 和 planSelectSampleDst时，传入的主键必须按原本的主键顺序!!!
        final List<String> pks = FastChecker.getorderedPrimaryKeys(tableMeta, ec);

        if (parallelism <= 0) {
            parallelism = Math.max(BackFillThreadPool.getInstance().getCorePoolSize() / 2, 1);
        }

        final int lockTimeOut = ec.getParamManager().getInt(ConnectionParams.FASTCHECKER_LOCK_TIMEOUT);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new MoveTableFastChecker(schemaName, tableName, tableName, srcPhyDbAndTables, dstPhyDbAndTables,
            allColumns, allColumns, pks, pks, parallelism, lockTimeOut,
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, pks, false, false),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, pks, false, true),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, pks, true, false),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, pks, true, true),

            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, pks, false, false),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, pks, false, true),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, pks, true, false),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, pks, true, true),

            builder.buildIdleSelectForChecker(tableMeta, allColumns),
            builder.buildIdleSelectForChecker(tableMeta, allColumns),

            builder.buildSqlSelectForSample(tableMeta, pks, pks, false, false),
            builder.buildSqlSelectForSample(tableMeta, pks, pks, false, false));
    }
}
