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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowTopology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class LogicalShowTopologyHandler extends HandlerCommon {
    public LogicalShowTopologyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowTopology showTopology = (SqlShowTopology) show.getNativeSqlNode();
        final SqlNode sqlTableName = showTopology.getTableName();

        String tableName = RelUtils.lastStringValue(sqlTableName);

        final OptimizerContext context = OptimizerContext.getContext(show.getSchemaName());
        //test the table existence
        TableMeta tableMeta = context.getLatestSchemaManager().getTable(tableName);

        final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo != null) {
            return handlePartitionedTable(partitionInfo);
        } else {
            return handleDRDSTable(executionContext, show);
        }

    }

    private Cursor handleDRDSTable(ExecutionContext executionContext, LogicalShow show) {
        final SqlShowTopology showTopology = (SqlShowTopology) show.getNativeSqlNode();
        final SqlNode sqlTableName = showTopology.getTableName();
        final OptimizerContext context = OptimizerContext.getContext(show.getSchemaName());
        TddlRuleManager tddlRuleManager = executionContext.getSchemaManager(show.getSchemaName()).getTddlRuleManager();
        String tableName = RelUtils.lastStringValue(sqlTableName);

        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        List<TargetDB> topology = tddlRuleManager.shard(tableName, true, true, null, null, calcParams,
            executionContext);

        ArrayResultCursor result = new ArrayResultCursor("TOPOLOGY");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("PARTITION_NAME", DataTypes.StringType);
        result.initMeta();

        String schemaName =
            TStringUtil.isNotEmpty(show.getSchemaName()) ? show.getSchemaName() : executionContext.getSchemaName();
        boolean isTableWithoutPrivileges = !CanAccessTable.verifyPrivileges(
            schemaName,
            tableName,
            executionContext);
        if (isTableWithoutPrivileges) {
            return result;
        }

        int index = 0;
        List<String> dbs = new ArrayList<String>();
        Map<String, List<String>> tbs = new HashMap<String, List<String>>(4);
        for (TargetDB db : topology) {
            List<String> oneTbs = new ArrayList<String>();
            for (String tn : db.getTableNames()) {
                oneTbs.add(tn);
            }

            Collections.sort(oneTbs);
            dbs.add(db.getDbIndex());
            tbs.put(db.getDbIndex(), oneTbs);
        }

        Collections.sort(dbs);
        for (String db : dbs) {
            for (String tn : tbs.get(db)) {
                result.addRow(new Object[] {index++, db, tn, "-"});
            }
        }

        return result;
    }

    private Cursor handlePartitionedTable(PartitionInfo partitionInfo) {
        ArrayResultCursor result = new ArrayResultCursor("TOPOLOGY");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("PARTITION_NAME", DataTypes.StringType);

        result.initMeta();
        int index = 0;
        Map<String, List<PhysicalPartitionInfo>> physicalPartitionInfos =
            partitionInfo.getPhysicalPartitionTopology(new ArrayList<>());

        for (Map.Entry<String, List<PhysicalPartitionInfo>> phyPartItem : physicalPartitionInfos.entrySet()) {
            String grpGroupKey = phyPartItem.getKey();
            List<PhysicalPartitionInfo> phyPartList = phyPartItem.getValue();
            for (int i = 0; i < phyPartList.size(); i++) {
                result.addRow(
                    new Object[] {
                        index++, grpGroupKey, phyPartList.get(i).getPhyTable(), phyPartList.get(i).getPartName()});
            }
        }
        return result;
    }
}
