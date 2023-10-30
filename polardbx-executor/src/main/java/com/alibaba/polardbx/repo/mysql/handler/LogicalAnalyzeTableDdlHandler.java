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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AnalyzeTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAnalyzeTable;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlAnalyzeTableDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper.deSerializeTask;

/**
 * @author wumu
 */
public class LogicalAnalyzeTableDdlHandler extends LogicalCommonDdlHandler {
    private static final Logger logger = LoggerFactory.getLogger("STATISTICS");

    private final int ANALYZE_TABLE_DEFAULT_SPEED = 100000000;

    public LogicalAnalyzeTableDdlHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        if (executionContext.getExtraCmds().get(ConnectionProperties.ANALYZE_TABLE_SPEED_LIMITATION) == null) {
            executionContext.getExtraCmds().put(ConnectionProperties.ANALYZE_TABLE_SPEED_LIMITATION,
                ANALYZE_TABLE_DEFAULT_SPEED);
        }

        LogicalAnalyzeTable logicalAnalyzeTable = (LogicalAnalyzeTable) logicalDdlPlan;
        final SqlAnalyzeTableDdl analyzeTable = (SqlAnalyzeTableDdl) logicalAnalyzeTable.getNativeSqlNode();
        List<Pair<String, String>> tableNameList =
            extractTableList(analyzeTable.getTableNames(), executionContext.getSchemaName(), executionContext);

        ExecutableDdlJob result = new ExecutableDdlJob();
        if (!tableNameList.isEmpty()) {
            AnalyzeTablePhyDdlTask analyzeTablePhyDdlTask = new AnalyzeTablePhyDdlTask(
                tableNameList.stream().map(Pair::getKey).collect(Collectors.toList()),
                tableNameList.stream().map(Pair::getValue).collect(Collectors.toList()),
                new ArrayList<>(),
                new ArrayList<>()
            );
            result.addTask(analyzeTablePhyDdlTask);

            result.labelAsTail(analyzeTablePhyDdlTask);
            for (Pair<String, String> targetTable : tableNameList) {
                final String fullTableName = DdlJobFactory.concatWithDot(targetTable.getKey(), targetTable.getValue());
                result.addExcludeResources(Sets.newHashSet(fullTableName));
            }
        }

        return result;
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation logicalDdlPlan, DdlJob ddlJob, ExecutionContext ec) {
        ArrayResultCursor result = new ArrayResultCursor("analyzeTable");
        result.addColumn("Table", DataTypes.StringType);
        result.addColumn("Op", DataTypes.StringType);
        result.addColumn("Msg_type", DataTypes.StringType);
        result.addColumn("Msg_text", DataTypes.StringType);
        result.initMeta();

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DdlEngineTaskAccessor accessor = new DdlEngineTaskAccessor();
            accessor.setConnection(metaDbConn);

            Long jobId = ec.getDdlJobId();
            Long taskId = ((ExecutableDdlJob) ddlJob).getTail().getTaskId();
            DdlEngineTaskRecord record = accessor.query(jobId, taskId);
            if (record == null) {
                record = accessor.archiveQuery(jobId, taskId);
            }
            if (record != null) {
                AnalyzeTablePhyDdlTask task = (AnalyzeTablePhyDdlTask) deSerializeTask(record.name, record.value);
                List<String> schemaNames = task.getSchemaNames();
                List<String> tableNames = task.getTableNames();
                List<Boolean> useHill = task.getUseHll();
                List<Boolean> success = task.getSuccess();

                for (int i = 0; i < tableNames.size(); ++i) {
                    String schemaName = schemaNames.get(i);
                    String table = tableNames.get(i);
                    if (!useHill.get(i)) {
                        result.addRow(new Object[] {schemaName + "." + table, "analyze", "use hll", "false"});
                    }

                    if (success.get(i)) {
                        result.addRow(new Object[] {schemaName + "." + table, "analyze", "status", "OK"});
                    } else {
                        result.addRow(new Object[] {schemaName + "." + table, "analyze", "status", "FAIL"});
                    }
                }
            }
        } catch (Throwable ex) {
            // 从 metadb 获取结果失败，但是实际上 DDL 任务执行成功
            logger.info("analyze table get result failed", ex);
        }

        return result;
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

            List<String> gsiNames = GlobalIndexMeta.getPublishedIndexNames(table, schema, ec);
            if (CollectionUtils.isNotEmpty(gsiNames)) {
                for (String gsi : gsiNames) {
                    result.add(Pair.of(schema, gsi));
                }
            }
        }
        return result;
    }
}
