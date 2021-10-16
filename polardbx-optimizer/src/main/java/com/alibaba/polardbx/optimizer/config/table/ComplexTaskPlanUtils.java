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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ComplexTaskPlanUtils {

    public static final EnumSet<SqlKind> MODIFY_DML =
        //never push the replace dml in delete_only status
        EnumSet.of(/*SqlKind.REPLACE, */SqlKind.DELETE, SqlKind.UPDATE);

    public static boolean isModifyDML(SqlKind sqlKind) {
        return sqlKind.belongsTo(MODIFY_DML);
    }

    public static final String REPLICATE_SQL_LOG = "scale_out_sql";

    public static boolean canWrite(TableMeta tableMeta) {
        ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean =
            tableMeta.getComplexTaskTableMetaBean();
        if (complexTaskTableMetaBean == null) {
            return false;
        }
        return complexTaskTableMetaBean.canWrite();
    }

    public static boolean canWrite(TableMeta tableMeta, String partitionName) {
        ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean =
            tableMeta.getComplexTaskTableMetaBean();
        if (complexTaskTableMetaBean == null) {
            return false;
        }
        return complexTaskTableMetaBean.canWrite(partitionName);
    }

    public static boolean canDelete(TableMeta tableMeta) {
        return canWrite(tableMeta);
    }

    public static boolean isDeleteOnly(TableMeta tableMeta) {

        ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean =
            tableMeta.getComplexTaskTableMetaBean();
        if (complexTaskTableMetaBean == null) {
            return false;
        }
        return complexTaskTableMetaBean.allPartIsDeleteOnly();
    }

    public static boolean isDeleteOnly(TableMeta tableMeta, String partitionName) {
        ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean =
            tableMeta.getComplexTaskTableMetaBean();
        if (complexTaskTableMetaBean == null) {
            return false;
        }
        return complexTaskTableMetaBean.isDeleteOnly(partitionName);
    }

    public static boolean isReadyToPublish(TableMeta tableMeta) {
        ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean =
            tableMeta.getComplexTaskTableMetaBean();
        if (complexTaskTableMetaBean == null) {
            return false;
        }
        return complexTaskTableMetaBean.allPartIsReadyToPublic();
    }

    public static boolean isReadyToPublish(TableMeta tableMeta, String partitionName) {
        ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean =
            tableMeta.getComplexTaskTableMetaBean();
        if (complexTaskTableMetaBean == null) {
            return false;
        }
        return complexTaskTableMetaBean.isReadyToPulic(partitionName);
    }

    public static boolean isScaleOutWriteDebugOpen(ExecutionContext executionContext) {

        final String scaleOutDebugInfo =
            executionContext.getParamManager().getString(ConnectionParams.SCALE_OUT_WRITE_DEBUG);
        if (!TStringUtil.isEmpty(scaleOutDebugInfo) && scaleOutDebugInfo.equalsIgnoreCase("TRUE")) {
            return true;
        }
        return false;
    }

    /**
     * Check if dml need open multi-write
     */
    public static boolean checkNeedOpenMultiWrite(String schemaName,
                                                  String logicalTableName,
                                                  Set<String> partitionSet,
                                                  ExecutionContext executionContext) {

        // When scale out debug mode is open, return true directly
        if (isScaleOutWriteDebugOpen(executionContext)) {
            return true;
        }

        TableMeta tableMeta =
            executionContext.getSchemaManager(schemaName).getTable(logicalTableName);

        boolean canWriteVal = tableMeta.getComplexTaskTableMetaBean().canWrite();
        if (!canWriteVal) {
            return false;
        }

        if (canWriteVal) {
            final boolean scaleOutDmlPushDown =
                executionContext.getParamManager().getBoolean(ConnectionParams.SCALEOUT_DML_PUSHDOWN_OPTIMIZATION);

            if (!scaleOutDmlPushDown) {
                return canWriteVal;
            }

            // When find a table that is doing scale out
            TableRule tbRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
            if (tbRule != null && tbRule.isBroadcast()) {
                return canWriteVal;
            } else if (tableMeta.getPartitionInfo() != null && tableMeta.getPartitionInfo().isBroadcastTable()) {
                return canWriteVal;
            }
            for (String part : partitionSet) {
                if (ComplexTaskPlanUtils.canWrite(tableMeta, part)) {
                    return true;
                }
            }
            return false;
        }
        return canWriteVal;
    }

    public static boolean isScaleOutRunningOnTables(List<RelOptTable> tables, ExecutionContext ec) {
        if (null == tables) {
            return false;
        }

        return tables.stream().anyMatch(table -> {
            final org.apache.calcite.util.Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final String schemaName = qn.left;
            final String tableName = qn.right;
            final TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);

            final boolean replicateWritable = ComplexTaskPlanUtils.canWrite(tableMeta);

            return replicateWritable;
        });
    }

    public static String getPartNameFromGroupAndTable(TableMeta tableMeta, String groupName, String phyTableName) {
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo == null) {
            return StringUtils.EMPTY;
        }
        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
            PartitionLocation location = partitionSpec.getLocation();
            if (location.getPhyTableName().equalsIgnoreCase(phyTableName) && location.getGroupKey()
                .equalsIgnoreCase(groupName)) {
                return partitionSpec.getName();
            }
        }
        return StringUtils.EMPTY;
    }
}
