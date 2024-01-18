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

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ReplaceDifferentDBSingleTblWithPhyTblVisitor extends ReplaceTableNameWithSomethingVisitor {

    private List<TableId> logicalTables = new ArrayList<>();
    private List<TableId> physicalTables = new ArrayList<>();
    private Map<String, Set<String>> schemaPhysicalMapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public ReplaceDifferentDBSingleTblWithPhyTblVisitor(String defaultSchemaName,
                                                        ExecutionContext executionContext) {
        super(defaultSchemaName, executionContext);
    }

    @Override
    protected SqlNode buildSth(SqlNode sqlNode) {
        if (ConfigDataMode.isFastMock()) {
            return sqlNode;
        }
        if (!(sqlNode instanceof SqlIdentifier)) {
            return sqlNode;
        }

        String schemaName;
        final String logicalTableName;
        if (((SqlIdentifier) sqlNode).isSimple()) {
            schemaName = defaultSchemaName;
            logicalTableName = ((SqlIdentifier) sqlNode).getLastName();
        } else {
            ImmutableList<String> names = ((SqlIdentifier) sqlNode).names;
            schemaName = names.get(names.size() - 2);
            logicalTableName = ((SqlIdentifier) sqlNode).getLastName();
        }

        String physicalDBName;
        String physicalTableName;
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            PartitionInfo partitionInfo = OptimizerContext.getContext(schemaName)
                .getPartitionInfoManager()
                .getPartitionInfo(logicalTableName);
            if (partitionInfo == null) {
                physicalDBName = schemaName;
                physicalTableName = logicalTableName;
            } else {
                physicalDBName = GroupInfoUtil.buildPhysicalDbNameFromGroupName(
                    partitionInfo.getPartitionBy().getPartitions().get(0).getLocation().getGroupKey());
                physicalTableName = partitionInfo.getPrefixTableName();
            }
        } else {
            final TableRule tableRule = OptimizerContext.getContext(schemaName)
                .getRuleManager()
                .getTableRule(logicalTableName);
            if (tableRule == null) {
                physicalDBName = schemaName;
                physicalTableName = logicalTableName;
            } else {
                physicalDBName = tableRule.getDbNamePattern();
                physicalTableName = tableRule.getTbNamePattern();
            }
        }
        logicalTables.add(new TableId(schemaName, logicalTableName));
        physicalTables.add(new TableId(physicalDBName, physicalTableName));
        schemaPhysicalMapping.computeIfAbsent(schemaName, v -> new HashSet<>()).add(physicalDBName);
        return new SqlIdentifier(ImmutableList.of(physicalDBName, physicalTableName), SqlParserPos.ZERO);
    }

    @Override
    protected boolean addAliasForDelete(SqlNode delete) {
        if (delete instanceof SqlDelete) {
            return ((SqlDelete) delete).getSubQueryTableMap().keySet().stream().anyMatch(k -> k.isA(SqlKind.QUERY));
        }
        return super.addAliasForDelete(delete);
    }

    public List<TableId> getLogicalTables() {
        return logicalTables;
    }

    public List<TableId> getPhysicalTables() {
        return physicalTables;
    }

    public Map<String, Set<String>> getSchemaPhysicalMapping() {
        return schemaPhysicalMapping;
    }
}
