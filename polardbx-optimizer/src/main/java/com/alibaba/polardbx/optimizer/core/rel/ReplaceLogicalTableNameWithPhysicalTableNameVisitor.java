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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

public class ReplaceLogicalTableNameWithPhysicalTableNameVisitor extends ReplaceTableNameWithSomethingVisitor {

    private String schemaName;

    public ReplaceLogicalTableNameWithPhysicalTableNameVisitor(String defaultSchemaName,
                                                               ExecutionContext executionContext) {
        super(defaultSchemaName, executionContext);
        this.schemaName = defaultSchemaName;
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
        String physicalTableName;
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            PartitionInfo partitionInfo = OptimizerContext.getContext(schemaName)
                .getPartitionInfoManager()
                .getPartitionInfo(logicalTableName);
            if (partitionInfo == null) {
                return new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);
            }
            physicalTableName = partitionInfo.getPrefixTableName();
        } else {
            final TableRule tableRule = OptimizerContext.getContext(schemaName)
                .getRuleManager()
                .getTableRule(logicalTableName);
            if (tableRule == null) {
                return new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);
            }
            physicalTableName = tableRule.getTbNamePattern();
        }
        return new SqlIdentifier(physicalTableName, SqlParserPos.ZERO);
    }

    @Override
    protected boolean addAliasForDelete(SqlNode delete) {
        if (delete instanceof SqlDelete) {
            return ((SqlDelete) delete).getSubQueryTableMap().keySet().stream().anyMatch(k -> k.isA(SqlKind.QUERY));
        }

        return super.addAliasForDelete(delete);
    }
}
