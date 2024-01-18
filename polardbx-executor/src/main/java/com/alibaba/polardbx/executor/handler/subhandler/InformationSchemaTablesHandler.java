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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BigIntegerType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.commons.lang.StringUtils;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author shengyu
 */
public class InformationSchemaTablesHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaTablesHandler.class);

    public InformationSchemaTablesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTables;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        if (ConfigDataMode.isFastMock()) {
            return cursor;
        }
        InformationSchemaTables informationSchemaTables = (InformationSchemaTables) virtualView;
        Set<String> schemaNames = OptimizerContext.getActiveSchemaNames();

        List<Object> tableSchemaIndexValue =
            virtualView.getIndex().get(informationSchemaTables.getTableSchemaIndex());

        Object tableSchemaLikeValue =
            virtualView.getLike().get(informationSchemaTables.getTableSchemaIndex());

        List<Object> tableNameIndexValue =
            virtualView.getIndex().get(informationSchemaTables.getTableNameIndex());

        Object tableNameLikeValue =
            virtualView.getLike().get(informationSchemaTables.getTableNameIndex());

        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        // schemaIndex
        Set<String> indexSchemaNames = new HashSet<>();
        if (tableSchemaIndexValue != null && !tableSchemaIndexValue.isEmpty()) {
            for (Object obj : tableSchemaIndexValue) {
                if (obj instanceof RexDynamicParam) {
                    String schemaName = String.valueOf(params.get(((RexDynamicParam) obj).getIndex() + 1).getValue());
                    indexSchemaNames.add(schemaName.toLowerCase());
                } else if (obj instanceof RexLiteral) {
                    String schemaName = ((RexLiteral) obj).getValueAs(String.class);
                    indexSchemaNames.add(schemaName.toLowerCase());
                }
            }
            schemaNames = schemaNames.stream()
                .filter(schemaName -> indexSchemaNames.contains(schemaName.toLowerCase()))
                .collect(Collectors.toSet());
        }

        // schemaLike
        String schemaLike = null;
        if (tableSchemaLikeValue != null) {
            if (tableSchemaLikeValue instanceof RexDynamicParam) {
                schemaLike =
                    String.valueOf(params.get(((RexDynamicParam) tableSchemaLikeValue).getIndex() + 1).getValue());
            } else if (tableSchemaLikeValue instanceof RexLiteral) {
                schemaLike = ((RexLiteral) tableSchemaLikeValue).getValueAs(String.class);
            }
            if (schemaLike != null) {
                final String likeArg = schemaLike;
                schemaNames =
                    schemaNames.stream().filter(schemaName -> new Like(null, null).like(schemaName, likeArg)).collect(
                        Collectors.toSet());
            }
        }

        // tableIndex
        Set<String> indexTableNames = new HashSet<>();
        if (tableNameIndexValue != null && !tableNameIndexValue.isEmpty()) {
            for (Object obj : tableNameIndexValue) {
                ExecUtils.handleTableNameParams(obj, params, indexSchemaNames);
            }
        }

        // tableLike
        String tableLike = null;
        if (tableNameLikeValue != null) {
            if (tableNameLikeValue instanceof RexDynamicParam) {
                tableLike =
                    String.valueOf(params.get(((RexDynamicParam) tableNameLikeValue).getIndex() + 1).getValue());
            } else if (tableNameLikeValue instanceof RexLiteral) {
                tableLike = ((RexLiteral) tableNameLikeValue).getValueAs(String.class);
            }
        }

        BigIntegerType bigIntegerType = new BigIntegerType();

        boolean once = true;

        boolean enableLowerCase =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_LOWER_CASE_TABLE_NAMES);

        for (String schemaName : schemaNames) {
            SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();

            // groupName -> {(logicalTableName, physicalTableName)}
            Map<String, Set<Pair<String, String>>> groupToPair =
                virtualViewHandler.getGroupToPair(schemaName, indexTableNames, tableLike,
                    executionContext.isTestMode());

            for (String groupName : groupToPair.keySet()) {

                TGroupDataSource groupDataSource =
                    (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                        .getGroupExecutor(groupName).getDataSource();

                String actualDbName = groupDataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY)
                    .getDsConfHandle().getRunTimeConf().getDbName();

                Set<Pair<String, String>> collection = groupToPair.get(groupName);

                if (collection.isEmpty()) {
                    continue;
                }

                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("select * from information_schema.tables where table_schema = ");
                stringBuilder.append("'");
                stringBuilder.append(actualDbName.replace("'", "\\'"));
                stringBuilder.append("' and table_name in (");

                boolean first = true;

                // physicalTableName -> logicalTableName
                Map<String, String> physicalTableToLogicalTable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (Pair<String, String> pair : collection) {
                    String logicalTableName = pair.getKey();
                    String physicalTableName = pair.getValue();
                    physicalTableToLogicalTable.put(physicalTableName, logicalTableName);
                    if (!first) {
                        stringBuilder.append(", ");
                    }
                    first = false;
                    stringBuilder.append("'");
                    stringBuilder.append(physicalTableName.replace("'", "\\'"));
                    stringBuilder.append("'");
                }
                stringBuilder.append(")");

                // FIXME: NEED UNION INFORMATION TABLES?
                if (once) {
                    stringBuilder.append(
                        " union select * from information_schema.tables where table_schema = 'information_schema'");
                    once = false;
                }

                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = groupDataSource.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(stringBuilder.toString());
                    while (rs.next()) {

                        String logicalTableName;
                        String tableSchema;
                        long tableRows;
                        String autoPartition = "NO";
                        if (rs.getString("TABLE_SCHEMA").equalsIgnoreCase("information_schema")) {
                            tableSchema = rs.getString("TABLE_SCHEMA");
                            logicalTableName = rs.getString("TABLE_NAME");
                            tableRows = rs.getLong("TABLE_ROWS");
                        } else {
                            logicalTableName =
                                physicalTableToLogicalTable.get(rs.getString("TABLE_NAME"));
                            tableSchema = schemaName;
                            StatisticResult statisticResult =
                                StatisticManager.getInstance().getRowCount(schemaName, logicalTableName, false);
                            tableRows = statisticResult.getLongValue();
                            if (!CanAccessTable.verifyPrivileges(schemaName, logicalTableName, executionContext)) {
                                continue;
                            }

                            // do not skip GSI when getting statistics for GSI
                            if (!informationSchemaTables.includeGsi()) {
                                try {
                                    TableMeta tableMeta = schemaManager.getTable(logicalTableName);
                                    if (tableMeta.isAutoPartition()) {
                                        autoPartition = "YES";
                                    }
                                    if (tableMeta.isGsi()) {
                                        continue;
                                    }
                                } catch (Throwable t) {
                                    // ignore table not exists
                                }
                            }
                        }

                        long single_data_length = rs.getLong("DATA_LENGTH");
                        double scale = 1;
                        if (single_data_length != 0 && rs.getLong("AVG_ROW_LENGTH") > 0) {
                            scale = ((double) (tableRows * rs.getLong("AVG_ROW_LENGTH"))) / single_data_length;
                        }
                        long dataLength = (long) (single_data_length * scale);
                        long indexLength = (long) (rs.getLong("INDEX_LENGTH") * scale);
                        long dataFree = (long) (rs.getLong("DATA_FREE") * scale);
                        BigInteger avgRowLength = (BigInteger) rs.getObject("AVG_ROW_LENGTH");
                        // build info for file storage table
                        if (!rs.getString("TABLE_SCHEMA").equalsIgnoreCase("information_schema")) {
                            if (StatisticUtils.isFileStore(schemaName, logicalTableName)) {
                                Map<String, Long> statisticMap =
                                    StatisticUtils.getFileStoreStatistic(schemaName, logicalTableName);
                                tableRows = statisticMap.get("TABLE_ROWS");
                                dataLength = statisticMap.get("DATA_LENGTH");
                                indexLength = statisticMap.get("INDEX_LENGTH");
                                dataFree = statisticMap.get("DATA_FREE");
                                if (tableRows != 0) {
                                    avgRowLength = BigInteger.valueOf(dataLength / tableRows);
                                }
                            }

                        }

                        cursor.addRow(new Object[] {
                            rs.getObject("TABLE_CATALOG"),
                            enableLowerCase ? StringUtils.lowerCase(tableSchema) : tableSchema,
                            enableLowerCase ? StringUtils.lowerCase(logicalTableName) : logicalTableName,
                            rs.getObject("TABLE_TYPE"),
                            rs.getObject("ENGINE"),
                            rs.getObject("VERSION"),
                            rs.getObject("ROW_FORMAT"),
                            tableRows,
                            avgRowLength,
                            dataLength,
                            rs.getObject("MAX_DATA_LENGTH"),
                            indexLength,
                            dataFree,
                            rs.getObject("AUTO_INCREMENT"),
                            rs.getObject("CREATE_TIME"),
                            rs.getObject("UPDATE_TIME"),
                            rs.getObject("CHECK_TIME"),
                            rs.getObject("TABLE_COLLATION"),
                            rs.getObject("CHECKSUM"),
                            rs.getObject("CREATE_OPTIONS"),
                            rs.getObject("TABLE_COMMENT"),
                            autoPartition
                        });
                    }
                } catch (Throwable t) {
                    logger.error(t);
                } finally {
                    GeneralUtil.close(rs);
                    GeneralUtil.close(stmt);
                    GeneralUtil.close(conn);
                }
            }
        }

        return cursor;
    }
}
