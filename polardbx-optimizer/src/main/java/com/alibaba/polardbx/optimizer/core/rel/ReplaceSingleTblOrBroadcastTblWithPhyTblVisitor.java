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

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Map;
import java.util.TreeMap;

public class ReplaceSingleTblOrBroadcastTblWithPhyTblVisitor extends ReplaceTableNameWithSomethingVisitor {

    private String schemaName;
    protected Map<String, String> logTblToPhyTblMapping = new TreeMap<String, String>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    protected boolean onlyContainBroadcastTbl = true;
    protected String singleTblGroup = null;

    public ReplaceSingleTblOrBroadcastTblWithPhyTblVisitor(String defaultSchemaName,
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
            if (partitionInfo.isGsiSingleOrSingleTable()) {
                this.onlyContainBroadcastTbl = false;
                this.singleTblGroup = partitionInfo.getPartitionBy().getPartitions().get(0).getLocation().getGroupKey();
            }

            logTblToPhyTblMapping.put(logicalTableName, physicalTableName);
        } else {
            final TableRule tableRule = OptimizerContext.getContext(schemaName)
                .getRuleManager()
                .getTableRule(logicalTableName);
            if (tableRule == null) {
                return new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);
            }

            if (!tableRule.isBroadcast() && tableRule.getShardColumns().isEmpty()) {
                onlyContainBroadcastTbl = false;
                this.singleTblGroup = tableRule.getDbNamePattern();
            }
            physicalTableName = tableRule.getTbNamePattern();
            logTblToPhyTblMapping.put(logicalTableName, physicalTableName);
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

    public String getSchemaName() {
        return schemaName;
    }

    public Map<String, String> getLogTblToPhyTblMapping() {
        return logTblToPhyTblMapping;
    }

    public boolean isOnlyContainBroadcastTbl() {
        return onlyContainBroadcastTbl;
    }


    public String getSingleTblGroup() {
        return singleTblGroup;
    }

    public void setSingleTblGroup(String singleTblGroup) {
        this.singleTblGroup = singleTblGroup;
    }
}
