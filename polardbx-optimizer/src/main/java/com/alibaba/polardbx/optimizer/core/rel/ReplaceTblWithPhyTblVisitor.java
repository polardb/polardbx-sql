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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class ReplaceTblWithPhyTblVisitor extends ReplaceTableNameWithSomethingVisitor {

    private String schemaName;
    private boolean withSingleTbl;
    private boolean withPartitionTbl;
    private String uniqGroupName;

    public ReplaceTblWithPhyTblVisitor(String defaultSchemaName,
                                       ExecutionContext executionContext) {
        super(defaultSchemaName, executionContext);
        //Preconditions.checkArgument(!DbInfoManager.getInstance().isNewPartitionDb(defaultSchemaName));
        this.schemaName = defaultSchemaName;
        this.withSingleTbl = false;
        this.withPartitionTbl = false;
    }

    @Override
    protected SqlNode buildSth(SqlNode sqlNode) {
        if (ConfigDataMode.isFastMock()) {
            return sqlNode;
        }
        if (!(sqlNode instanceof SqlIdentifier)) {
            return sqlNode;
        }
        final String logicalTableName = ((SqlIdentifier) sqlNode).getLastName();

        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        if (tddlRuleManager == null) {
            return sqlNode;
        }
        String pName = ec.getPartitionName();
        final TableRule tableRule = tddlRuleManager.getTableRule(logicalTableName);
        if (tableRule == null) {
            if (pName != null && !pName.isEmpty()) {
                TableMeta tb =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
                PartitionInfo pi = tb.getPartitionInfo();
                List<String> partitionList = Lists.newArrayList();
                partitionList.add(pName);
                Map<String, List<PhysicalPartitionInfo>> physicalPartitionInfos =
                    pi.getPhysicalPartitionTopology(partitionList);
                if (physicalPartitionInfos != null && physicalPartitionInfos.size() == 1) {
                    Map.Entry<String, List<PhysicalPartitionInfo>> entry =
                        physicalPartitionInfos.entrySet().iterator().next();
                    String groupName = entry.getKey();
                    if (entry.getValue().size() == 1) {
                        if (uniqGroupName == null) {
                            uniqGroupName = groupName.toLowerCase(Locale.ROOT).trim();
                        } else {
                            // error
                            if (!uniqGroupName.equalsIgnoreCase(groupName)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                                    "Unsupported to use direct HINT for multi partition");
                            }
                        }
                        String physicalTblName = entry.getValue().get(0).getPhyTable();
                        return new SqlIdentifier(physicalTblName, SqlParserPos.ZERO);
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                        "Unsupported to use direct HINT for part table that has multi physical tables in one partition");
                }
            }

            return sqlNode;
        }
        // don't replace table with sharding db and  tb
        if (tableRule.getActualTopology().size() > 1) {
            for (Map.Entry<String, Set<String>> dbEntry : tableRule.getActualTopology().entrySet()) {
                if (dbEntry.getValue().size() > 1) {
                    return new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);
                }
            }
        }
        if (pName != null && !pName.isEmpty()) {
            if (uniqGroupName == null) {
                uniqGroupName = pName;
            } else {
                // error
            }
        }

        this.withSingleTbl |= tddlRuleManager.isTableInSingleDb(logicalTableName);
        this.withPartitionTbl |= tddlRuleManager.isShard(logicalTableName);

        return new SqlIdentifier(tableRule.getTbNamePattern(), SqlParserPos.ZERO);
    }

    @Override
    protected boolean addAliasForDelete(SqlNode delete) {
        if (delete instanceof SqlDelete) {
            return ((SqlDelete) delete).getSubQueryTableMap().keySet().stream().anyMatch(k -> k.isA(SqlKind.QUERY));
        }

        return super.addAliasForDelete(delete);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean shouldChooseSingleGroup() {
        // with single table and without partition table
        if (withSingleTbl && !withPartitionTbl) {
            return true;
        }
        return false;
    }

    public String getUniqGroupName() {
        return uniqGroupName;
    }
}
