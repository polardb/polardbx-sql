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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.builder.DirectPhysicalSqlPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.OptimizeTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobUtil;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalOptimizeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ReorganizeLocalPartitionPreparedData;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOptimizeTableDdl;
import org.apache.calcite.sql.SqlPhyDdlWrapper;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OPTIMIZE TABLE
 *
 * @author guxu.ygh
 * @since 2022/05
 */
public class LogicalOptimizeTableHandler extends LogicalCommonDdlHandler {

    public LogicalOptimizeTableHandler(IRepository repo) {
        super(repo);
    }

    private static final String OPTIMIZE_TABLE_DDL_TEMPLATE = "optimize table ?";

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext ec) {
        LogicalOptimizeTable logicalOptimizeTable = (LogicalOptimizeTable) logicalDdlPlan;
        SqlOptimizeTableDdl sqlOptimizeTableDdl = (SqlOptimizeTableDdl) logicalOptimizeTable.getNativeSqlNode();
        List<Pair<String, String>> tableNameList =
            extractTableList(sqlOptimizeTableDdl.getTableNames(), ec.getSchemaName(), ec);
        final int parallelism = ec.getParamManager().getInt(ConnectionParams.OPTIMIZE_TABLE_PARALLELISM);
        if (parallelism < 1 || parallelism > 4096) {
            throw new TddlNestableRuntimeException("OPTIMIZE_TABLE_PARALLELISM must in range 0-4096");
        }

        ExecutableDdlJob result = new ExecutableDdlJob();

        for (Pair<String, String> targetTable : tableNameList) {
            OptimizeTablePhyDdlTask phyDdlTask =
                genPhyDdlTask(logicalDdlPlan.relDdl, targetTable.getKey(), targetTable.getValue(),
                    OPTIMIZE_TABLE_DDL_TEMPLATE, ec);
            final String fullTableName = DdlJobFactory.concatWithDot(targetTable.getKey(), targetTable.getValue());
            ExecutableDdlJob job = new ExecutableDdlJob();
            job.addSequentialTasks(Lists.newArrayList(phyDdlTask.partition(parallelism)));
            job.addExcludeResources(Sets.newHashSet(fullTableName));
            result.appendJob2(job);
        }

        return result;
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation logicalDdlPlan, DdlJob ddlJob, ExecutionContext ec) {

        if (ec.getDdlContext().isAsyncMode()) {
            return super.buildResultCursor(logicalDdlPlan, ddlJob, ec);
        }

        ArrayResultCursor result = new ArrayResultCursor("OptimizeTable");
        result.addColumn("Table", DataTypes.StringType);
        result.addColumn("Op", DataTypes.StringType);
        result.addColumn("Msg_type", DataTypes.StringType);
        result.addColumn("Msg_text", DataTypes.StringType);

        LogicalOptimizeTable logicalOptimizeTable = (LogicalOptimizeTable) logicalDdlPlan;
        SqlOptimizeTableDdl sqlOptimizeTableDdl = (SqlOptimizeTableDdl) logicalOptimizeTable.getNativeSqlNode();
        List<Pair<String, String>> tableNameList =
            extractTableList(sqlOptimizeTableDdl.getTableNames(), ec.getSchemaName(), ec);

        for (Pair<String, String> targetTable : tableNameList) {
            final String fullTableName = DdlJobFactory.concatWithDot(targetTable.getKey(), targetTable.getValue());
            result.addRow(new Object[] {
                fullTableName,
                "optimize",
                "note",
                "Table does not support optimize, doing recreate + analyze instead"
            });
            result.addRow(new Object[] {
                fullTableName,
                "optimize",
                "status",
                "OK"
            });
        }

        return result;
    }

    private OptimizeTablePhyDdlTask genPhyDdlTask(DDL ddl, String schemaName, String tableName, String phySql,
                                                  ExecutionContext executionContext) {
        ddl.sqlNode =
            SqlPhyDdlWrapper.createForAllocateLocalPartition(new SqlIdentifier(tableName, SqlParserPos.ZERO), phySql);
        DirectPhysicalSqlPlanBuilder builder = new DirectPhysicalSqlPlanBuilder(
            ddl, new ReorganizeLocalPartitionPreparedData(schemaName, tableName), executionContext
        );
        builder.build();
        OptimizeTablePhyDdlTask phyDdlTask = new OptimizeTablePhyDdlTask(schemaName, builder.genPhysicalPlanData());
        return phyDdlTask;
    }

    private List<Pair<String, String>> extractTableList(List<SqlNode> tableNameSqlNodeList, String currentSchemaName,
                                                        ExecutionContext ec) {
        if (CollectionUtils.isEmpty(tableNameSqlNodeList)) {
            return new ArrayList<>();
        }
        List<Pair<String, String>> result = new ArrayList<>();
        for (SqlNode sqlNode : tableNameSqlNodeList) {
            String schema = currentSchemaName;
            if (!((SqlIdentifier) sqlNode).isSimple()) {
                schema = ((SqlIdentifier) sqlNode).names.get(0);
            }
            String table = ((SqlIdentifier) sqlNode).getLastName();
            result.add(Pair.of(schema, table));

//            List<String> gsiNames = GlobalIndexMeta.getPublishedIndexNames(table, schema, ec);
//            if (CollectionUtils.isNotEmpty(gsiNames)) {
//                for (String gsi : gsiNames) {
//                    result.add(Pair.of(schema, gsi));
//                }
//            }

            /**
             * The target table names of optimize table should ignore cci
             */
            final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(table);
            Map<String, GsiMetaManager.GsiIndexMetaBean> gsiBeans = tableMeta.getGsiPublished();
            if (gsiBeans != null && !gsiBeans.isEmpty()) {
                for (String gsiName : gsiBeans.keySet()) {
                    GsiMetaManager.GsiIndexMetaBean gsiBean = gsiBeans.get(gsiName);
                    if (gsiBean != null) {

                        continue;
                    }
                    result.add(Pair.of(schema, gsiName));
                }
            }
        }
        return result;
    }

}
