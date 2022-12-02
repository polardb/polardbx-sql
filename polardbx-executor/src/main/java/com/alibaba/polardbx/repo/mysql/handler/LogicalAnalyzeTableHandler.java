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

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IDataSourceGetter;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.executor.utils.SchemaMetaUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAnalyzeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_HLL;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.forceAnalyzeColumns;

/**
 * @author chenmo.cm
 */
public class LogicalAnalyzeTableHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger("statistics");

    public LogicalAnalyzeTableHandler(IRepository repo) {
        super(repo);
    }

    public final String ANALYZE_TABLE_SQL = "ANALYZE TABLE ";
    private final int ANALYZE_TABLE_DEFAULT_SPEED = 100000000;

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDal dal = (LogicalDal) logicalPlan;
        final SqlAnalyzeTable analyzeTable = (SqlAnalyzeTable) dal.getNativeSqlNode();

        if (executionContext.getExtraCmds().get(ConnectionProperties.ANALYZE_TABLE_SPEED_LIMITATION) == null) {
            executionContext.getExtraCmds().put(ConnectionProperties.ANALYZE_TABLE_SPEED_LIMITATION,
                ANALYZE_TABLE_DEFAULT_SPEED);
        }

        String defaultSchemaName = executionContext.getSchemaName();

        List<Pair<String, String>> schemaTables = new ArrayList<>();
        for (SqlNode tableSqlNode : analyzeTable.getTableNames()) {
            String schemaName = defaultSchemaName;
            String tableName;
            if (tableSqlNode instanceof SqlIdentifier) {
                if (((SqlIdentifier) tableSqlNode).names.size() == 2) {
                    schemaName = ((SqlIdentifier) tableSqlNode).names.get(0);
                }
            }
            tableName = Util.last(((SqlIdentifier) tableSqlNode).names);
            OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
            if (optimizerContext == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
            }
            schemaTables.add(Pair.of(schemaName, tableName));
        }

        ArrayResultCursor result = new ArrayResultCursor("analyzeTable");
        result.addColumn("Table", DataTypes.StringType);
        result.addColumn("Op", DataTypes.StringType);
        result.addColumn("Msg_type", DataTypes.StringType);
        result.addColumn("Msg_text", DataTypes.StringType);
        result.initMeta();

        long start = System.currentTimeMillis();
        for (Pair<String, String> pair : schemaTables) {
            String schemaName = pair.getKey();
            String table = pair.getValue();

            IDataSourceGetter mysqlDsGetter = new MyDataSourceGetter(schemaName);

            TableMeta tableMeta;
            try {
                if (ConfigDataMode.isMasterMode()) {
                    doAnalyzeOneLogicalTable(schemaName, table, mysqlDsGetter, executionContext);
                }
                tableMeta =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(table);
            } catch (TableNotFoundException e) {
                result.addRow(new Object[] {schemaName + "." + table, "analyze", "status", "NO TABLE"});
                continue;
            }

            if (tableMeta == null) {
                result.addRow(new Object[] {schemaName + "." + table, "analyze", "status", "NO TABLE"});
                continue;
            } else {
                if (!executionContext.getParamManager().getBoolean(ENABLE_HLL) || !SchemaMetaUtil
                    .checkSupportHll(schemaName)) {
                    result.addRow(new Object[] {schemaName + "." + table, "analyze", "use hll", "false"});
                }
            }
            if (OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(table) == null) {
                logger.warn(
                    "no table rule for logicalTableName = " + table + ", analyze this table as the single table!");
            }

            boolean success = forceAnalyzeColumns(schemaName, table);
            if (success) {
                result.addRow(new Object[] {schemaName + "." + table, "analyze", "status", "OK"});
            } else {
                result.addRow(new Object[] {schemaName + "." + table, "analyze", "status", "FAIL"});
            }

            // refresh plancache
            PlanCache.getInstance().invalidate(table);
        }
        long end = System.currentTimeMillis();
        ModuleLogInfo.getInstance()
            .logRecord(Module.STATISTIC,
                LogPattern.PROCESS_END,
                new String[] {
                    "analyze table " + schemaTables.stream().map(pair -> {
                        String schemaName = pair.getKey() == null ? defaultSchemaName : pair.getKey();
                        String tableName = pair.getValue();
                        return schemaName + "." + tableName;
                    }).collect(Collectors.joining(",")),
                    "consuming " + (end - start) / 1000.0 + " seconds " + executionContext.getTraceId()
                },
                LogLevel.NORMAL);
        return result;
    }

    protected void doAnalyzeOneLogicalTable(String schemaName, String logicalTableName,
                                            IDataSourceGetter mysqlDsGetter, ExecutionContext executionContext) {
        List<Pair<String, String>> keys =
            StatisticManager.getInstance().buildStatisticKey(schemaName, logicalTableName, executionContext);
        for (Pair<String, String> key : keys) {
            String group = key.getKey();
            String physicalTableName = key.getValue();
            doAnalyzeOnePhysicalTable(group, physicalTableName, mysqlDsGetter);
        }
    }

    protected void doAnalyzeOnePhysicalTable(String group, String physicalTableName, IDataSourceGetter mysqlDsGetter) {
        if (StringUtils.isEmpty(physicalTableName)) {
            return;
        }
        physicalTableName = physicalTableName.toLowerCase();
        DataSource ds = mysqlDsGetter.getDataSource(group);
        if (ds == null) {
            logger.error("Analyze physical table " + physicalTableName
                + " cannot be fetched, datasource is null, group name is " + group);
            return;
        }
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ds.getConnection();
            stmt = conn.prepareStatement(ANALYZE_TABLE_SQL + physicalTableName);
            stmt.execute();
        } catch (Exception e) {
            logger.error("Analyze physical table " + physicalTableName + " ERROR");
            return;
        } finally {
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }
}
