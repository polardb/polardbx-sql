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

package com.alibaba.polardbx.executor.gsi.corrector;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import lombok.Value;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class GsiChecker extends Checker {

    /**
     * Parameters of GsiCheckers
     */
    @Value
    public static class Params {
        long batchSize;
        long speedMin;
        long speedLimit;
        long parallelism;
        long earlyFailNumber;

        public static Params buildFromExecutionContext(ExecutionContext ec) {
            ParamManager pm = ec.getParamManager();

            return new Params(
                pm.getLong(ConnectionParams.GSI_CHECK_BATCH_SIZE),
                pm.getLong(ConnectionParams.GSI_CHECK_SPEED_MIN),
                pm.getLong(ConnectionParams.GSI_CHECK_SPEED_LIMITATION),
                pm.getLong(ConnectionParams.GSI_CHECK_PARALLELISM),
                pm.getLong(ConnectionParams.GSI_EARLY_FAIL_NUMBER)
            );
        }
    }

    public GsiChecker(String schemaName, String tableName, String indexName,
                      TableMeta primaryTableMeta, TableMeta gsiTableMeta,
                      Params params,
                      SqlSelect.LockMode primaryLock, SqlSelect.LockMode gsiLock,
                      PhyTableOperation planSelectWithMaxPrimary,
                      PhyTableOperation planSelectWithMaxGsi,
                      PhyTableOperation planSelectWithMinAndMaxPrimary,
                      PhyTableOperation planSelectWithMinAndMaxGsi, SqlSelect planSelectWithInTemplate,
                      PhyTableOperation planSelectWithIn, PhyTableOperation planSelectMaxPk,
                      List<String> indexColumns, BitSet primaryKeys,
                      Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator) {
        super(schemaName, tableName, indexName, primaryTableMeta, gsiTableMeta,
            params.getBatchSize(), params.getSpeedMin(), params.getSpeedLimit(), params.getParallelism(),
            primaryLock, gsiLock, planSelectWithMaxPrimary, planSelectWithMaxGsi, planSelectWithMinAndMaxPrimary,
            planSelectWithMinAndMaxGsi, planSelectWithInTemplate, planSelectWithIn, planSelectMaxPk, indexColumns,
            primaryKeys, rowComparator);
    }

    public static Checker create(String schemaName, String tableName, String indexName,
                                 Params params,
                                 SqlSelect.LockMode primaryLock, SqlSelect.LockMode gsiLock,
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

        List<String> primaryKeys = GlobalIndexMeta.getPrimaryKeys(baseTableMeta);
        final BitSet primaryKeySet = new BitSet(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            for (int i = 0; i < indexColumns.size(); i++) {
                if (primaryKey.equalsIgnoreCase(indexColumns.get(i))) {
                    primaryKeySet.set(i);
                }
            }
        }

        primaryKeys = primaryKeySet.stream().mapToObj(indexColumns::get).collect(Collectors.toList());

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        final Pair<SqlSelect, PhyTableOperation> selectWithIn = builder
            .buildSelectWithInForChecker(baseTableMeta, indexColumns, primaryKeys, true);

        final List<DataType> columnTypes = indexTableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getDataType)
            .collect(Collectors.toList());
        final Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator = (o1, o2) -> {
            int idx = 0;
            while ((idx = primaryKeySet.nextSetBit(idx)) >= 0) {
                int n = ExecUtils
                    .comp(o1.get(idx).getKey().getValue(), o2.get(idx).getKey().getValue(), columnTypes.get(idx), true);
                if (n != 0) {
                    return n;
                }
                ++idx;
            }
            return 0;
        };

        return new GsiChecker(schemaName,
            tableName,
            indexName,
            baseTableMeta,
            indexTableMeta,
            params,
            primaryLock,
            gsiLock,
            builder.buildSelectForBackfill(baseTableMeta, indexColumns, primaryKeys, false, true, primaryLock),
            builder.buildSelectForBackfill(baseTableMeta, indexColumns, primaryKeys, false, true, gsiLock),
            builder.buildSelectForBackfill(baseTableMeta, indexColumns, primaryKeys, true, true, primaryLock),
            builder.buildSelectForBackfill(baseTableMeta, indexColumns, primaryKeys, true, true, gsiLock),
            selectWithIn.getKey(),
            selectWithIn.getValue(),
            builder.buildSelectMaxPkForBackfill(baseTableMeta, primaryKeys),
            indexColumns,
            primaryKeySet,
            rowComparator);
    }
}
