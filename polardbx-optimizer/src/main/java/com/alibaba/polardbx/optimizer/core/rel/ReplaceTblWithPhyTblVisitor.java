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
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Map;
import java.util.Set;

public class ReplaceTblWithPhyTblVisitor extends ReplaceTableNameWithSomethingVisitor {

    private String schemaName;
    private boolean withSingleTbl;
    private boolean withPartitionTbl;

    public ReplaceTblWithPhyTblVisitor(String defaultSchemaName,
                                       ExecutionContext executionContext) {
        super(defaultSchemaName, executionContext);
        Preconditions.checkArgument(!DbInfoManager.getInstance().isNewPartitionDb(defaultSchemaName));
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
        final TableRule tableRule = tddlRuleManager.getTableRule(logicalTableName);
        if (tableRule == null) {
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

}
