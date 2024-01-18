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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.util.DbNameUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTableGroup;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlSubPartitionBy;

import java.sql.Connection;
import java.util.List;
import java.util.Random;

public class TableGroupValidator {

    public static void validateTableGroupInfo(String schemaName, String tableGroupName, boolean isAllowEmpty,
                                              ParamManager paramManager) {
        validateTableGroupNameLength(tableGroupName);

        checkIfTableGroupExists(schemaName, tableGroupName, isAllowEmpty);

        LimitValidator.validateTableCount(schemaName);
    }

    public static void validateTableGroupName(String tableGroupName) {
        validateTableGroupNameLength(tableGroupName);
        if (tableGroupName.equalsIgnoreCase(TableGroupNameUtil.SINGLE_DEFAULT_TG_NAME_TEMPLATE)
            || tableGroupName.equalsIgnoreCase(TableGroupNameUtil.BROADCAST_TG_NAME_TEMPLATE)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't create the tablegroup:[" + tableGroupName + "] which is used internally");
        }
        for (int i = 0; i < tableGroupName.length(); i++) {
            if (!DbNameUtil.isWord(tableGroupName.charAt(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format(
                        "Failed to execute this command because the tableGroupName[%s] contains some invalid characters",
                        tableGroupName));
            }
        }
    }

    public static void validateTableGroupNameLength(String tableGroupName) {
        LimitValidator.validateTableGroupNameLength(tableGroupName);
    }

    public static void validateTableGroupExistence(String schemaName, String tableGroupName) {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS, tableGroupName);
        }
    }

    public static void validateTableGroupIsEmpty(String schemaName, String tableGroupName) {
        validateTableGroupExistence(schemaName, tableGroupName);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        if (!tableGroupConfig.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("The tablegroup[%s] is not empty", tableGroupName));
        }
    }

    public static void checkIfTableGroupExists(String schemaName, String tableGroupName, boolean isAllowEmpty) {
        validateTableGroupExistence(schemaName, tableGroupName);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig.getTableCount() == 0 && !isAllowEmpty) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                tableGroupName + " when it's empty");
        }
    }

    public static void checkIfTargetTablesTopologyCompatibleWithLocality(String schemaName,
                                                                         String targetTablesTopology) {

    }

    public static void validatePhysicalGroupIsNormal(String schemaName, String group) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(connection);
            DbGroupInfoRecord dbGroupInfoRecord =
                dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(schemaName, group, false);
            if (dbGroupInfoRecord != null) {
                if (dbGroupInfoRecord.groupType != DbGroupInfoRecord.GROUP_TYPE_NORMAL) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PHYSICAL_TOPOLOGY_CHANGING,
                        String.format("the physical group[%s] is changing, please retry this command later",
                            group));
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PHYSICAL_TOPOLOGY_CHANGING,
                    String.format("the physical group[%s] is changing, please retry this command later",
                        group));
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static void validatePartitionDefs(SqlCreateTableGroup sqlCreateTableGroup, ExecutionContext ec) {
        SqlPartitionBy sqlPartitionBy = (SqlPartitionBy) sqlCreateTableGroup.getSqlPartition();
        if (sqlPartitionBy == null) {
            return;
        }
        SQLPartitionBy fastSqlPartitionBy = FastsqlUtils.parsePartitionBy(sqlPartitionBy.toString(), true);
        SQLSubPartitionBy fastSqlSubPartitionBy = fastSqlPartitionBy.getSubPartitionBy();
        fastSqlPartitionBy.setForTableGroup(false);

        String tableName;
        Random rand = new Random();

        do {
            tableName = String.format("_for_explain_%05d", rand.nextInt(100000));
            try {
                ec.getSchemaManager().getTable(tableName);
            } catch (TableNotFoundException ex) {
                break;
            }
        } while (true);
        StringBuilder sb = new StringBuilder();
        sb.append("explain create table ");
        sb.append(tableName);
        sb.append("(");
        int i = 0;
        boolean firstCol = true;
        boolean handleSubPartition = false;
        List<SqlNode> columnsDef = sqlPartitionBy.getColumnsDefinition();
        do {
            for (SqlNode columnDef : columnsDef) {
                if (!firstCol) {
                    sb.append(", ");
                } else {
                    firstCol = false;
                }
                String colName = "c" + i;
                sb.append(colName);
                sb.append(" ");
                sb.append(((SqlColumnDeclaration) columnDef).getDataType().toString());
                i++;
                if (!handleSubPartition) {
                    fastSqlPartitionBy.addColumn(new SQLIdentifierExpr(colName));
                } else {
                    fastSqlSubPartitionBy.addColumn(new SQLIdentifierExpr(colName));
                }
            }
            if (!handleSubPartition) {
                SqlSubPartitionBy subPartitionBy = sqlPartitionBy.getSubPartitionBy();
                if (subPartitionBy != null) {
                    columnsDef = subPartitionBy.getColumnsDefinition();
                    fastSqlSubPartitionBy.setForTableGroup(false);
                } else {
                    columnsDef = null;
                }
                handleSubPartition = true;
            } else {
                columnsDef = null;
            }
        } while (columnsDef != null);
        sb.append(") PARTITION BY ");

        sb.append(
            SQLUtils.toSQLString(fastSqlPartitionBy, DbType.mysql, null, VisitorFeature.OutputHashPartitionsByRange));

        DdlHelper.getServerConfigManager().executeBackgroundSql(sb.toString(), ec.getSchemaName(), null);
    }
}
