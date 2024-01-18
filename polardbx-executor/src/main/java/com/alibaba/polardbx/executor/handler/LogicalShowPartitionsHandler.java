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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ExecutorCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaPartitionsMetaHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowPartitions;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class LogicalShowPartitionsHandler extends HandlerCommon {
    public LogicalShowPartitionsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowPartitions showPartitions = (SqlShowPartitions) show.getNativeSqlNode();

        String schemaName = ((LogicalShow) logicalPlan).getSchemaName();
        String tableName = RelUtils.lastStringValue(showPartitions.getTableName());
        String indexName = "";
        String targetGsiName = null;
        PartitionInfo gsiPartInfo = null;

        final OptimizerContext context = OptimizerContext.getContext(schemaName);

        PartitionInfoManager partMgr = context.getRuleManager().getPartitionInfoManager();
        PartitionInfo partInfo = partMgr.getPartitionInfo(tableName);
        boolean isNewPartTbl = partInfo != null;
        TableRule tableRule = context.getRuleManager().getTableRule(tableName);
        if (isNewPartTbl) {
            if (showPartitions.isShowIndexPartMeta()) {
                TableMeta tm = context.getLatestSchemaManager().getTable(tableName);
                indexName = RelUtils.lastStringValue(showPartitions.getIndexName());
                Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = tm.getGsiPublished();
                if (gsiPublished != null) {
                    String idxNameLowerCase = indexName.toLowerCase();
                    // all gsi tbl name format is idxname + "_$xxxx";
                    int targetLength = idxNameLowerCase.length() + 6;
                    for (String gsiName : gsiPublished.keySet()) {
                        if (gsiName.toLowerCase().startsWith(idxNameLowerCase) && gsiName.length() == targetLength) {
                            targetGsiName = gsiName;
                            break;
                        }
                    }
                }

                if (targetGsiName == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        String.format("Not found the index [%s] of table [%s]", indexName, tableName));
                }
                gsiPartInfo = partMgr.getPartitionInfo(targetGsiName);

                if (gsiPartInfo == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        String.format("Not found the index meta [%s] of table [%s]", indexName, tableName));
                }
            }
        }

        ArrayResultCursor result = new ArrayResultCursor("PARTITIONS");
        if (!isNewPartTbl) {
            result.addColumn("KEYS", DataTypes.StringType);
        } else {
            result.addColumn("PART_NUM", DataTypes.LongType);

            result.addColumn("TABLE_SCHEMA", DataTypes.StringType);
            result.addColumn("TABLE_NAME", DataTypes.StringType);
            result.addColumn("INDEX_NAME", DataTypes.StringType);
            result.addColumn("PRIM_TABLE", DataTypes.StringType);
            result.addColumn("TABLE_TYPE", DataTypes.StringType);
            result.addColumn("TG_NAME", DataTypes.StringType);

            result.addColumn("PART_METHOD", DataTypes.StringType);
            result.addColumn("PART_COL", DataTypes.StringType);
            result.addColumn("PART_COL_TYPE", DataTypes.StringType);
            result.addColumn("PART_EXPR", DataTypes.StringType);
            //result.addColumn("PART_BOUND_TYPE", DataTypes.StringType);

            result.addColumn("PART_NAME", DataTypes.StringType);
            result.addColumn("PART_POSI", DataTypes.LongType);
            result.addColumn("PART_DESC", DataTypes.StringType);

            result.addColumn("SUBPART_METHOD", DataTypes.StringType);
            result.addColumn("SUBPART_COL", DataTypes.StringType);
            result.addColumn("SUBPART_COL_TYPE", DataTypes.StringType);
            result.addColumn("SUBPART_EXPR", DataTypes.StringType);
            //result.addColumn("SUBPART_BOUND_TYPE", DataTypes.StringType);

            result.addColumn("SUBPART_NAME", DataTypes.StringType);
            result.addColumn("SUBPART_TEMP_NAME", DataTypes.StringType);
            result.addColumn("SUBPART_POSI", DataTypes.LongType);
            result.addColumn("SUBPART_DESC", DataTypes.StringType);

            result.addColumn("PG_NAME", DataTypes.StringType);
            result.addColumn("PHY_GROUP", DataTypes.StringType);
            result.addColumn("PHY_DB", DataTypes.StringType);
            result.addColumn("PHY_TB", DataTypes.StringType);
            result.addColumn("RW_DN", DataTypes.StringType);
        }

        result.initMeta();

        schemaName = TStringUtil.isNotEmpty(schemaName) ? schemaName : executionContext.getSchemaName();
        boolean isTableWithoutPrivileges = !CanAccessTable.verifyPrivileges(
            schemaName,
            tableName,
            executionContext);
        if (isTableWithoutPrivileges) {
            return result;
        }

        if (!isNewPartTbl) {
            return handleForDrdsMode(result, tableRule);
        } else {
            if (showPartitions.isShowIndexPartMeta()) {
                return handleForAutoMode(result, gsiPartInfo, indexName, tableName);
            } else {
                return handleForAutoMode(result, partInfo, "", tableName);
            }

        }

    }

    protected Cursor handleForDrdsMode(ArrayResultCursor result, TableRule tableRule) {
        if (tableRule == null) {
            result.addRow(new Object[] {"null"});
        } else {
            String keys = StringUtils.join(tableRule.getShardColumns(), ",");
            result.addRow(new Object[] {keys});
        }
        return result;
    }

    protected Cursor handleForAutoMode(ArrayResultCursor result,
                                       PartitionInfo partInfo,
                                       String indexName,
                                       String primaryTableName) {
        InformationSchemaPartitionsMetaHandler.handlePartitionsMeta(result, partInfo, indexName, primaryTableName);
        return result;
    }

}
