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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabaseItemPreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.sharding.DataNodeChooser;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.*;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveDatabaseItemBuilder extends DdlPhyPlanBuilder {

    private final MoveDatabaseItemPreparedData preparedData;

    private final ExecutionContext executionContext;
    private Map<String, Set<String>> targetPhyTables = new LinkedHashMap<>();
    private Map<String, Set<String>> sourcePhyTables = new LinkedHashMap<>();
    private Pair<String, String> defaultGroupAndPhyTable;

    public MoveDatabaseItemBuilder(DDL ddl,
                                   MoveDatabaseItemPreparedData preparedData,
                                   ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    public MoveDatabaseItemBuilder build() {
        buildTableRuleAndTopology();
        buildPhysicalPlans();
        this.built = true;
        return this;
    }

    @Override
    protected void buildTableRuleAndTopology() {
        buildExistingTableRule(preparedData.getTableName());
        buildNewTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
        buildAlterReferenceTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
    }

    @Override
    protected void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

    public Map<String, Set<String>> getTargetPhyTables() {
        return targetPhyTables;
    }

    public Map<String, Set<String>> getSourcePhyTables() {
        return sourcePhyTables;
    }

    @Override
    protected void buildNewTableTopology(String schemaName, String tableName) {
        tableTopology = new TreeMap<>();
        for (Map.Entry<String, String> sourceTargetGroup : preparedData.getSourceTargetGroupMap().entrySet()) {
            List<String> phyTableNames =
                ScaleOutPlanUtil
                    .getPhysicalTables(sourceTargetGroup.getKey(), preparedData.getSchemaName(),
                        preparedData.getTableName());
            assert GeneralUtil.isNotEmpty(phyTableNames);
            for (String newPhyTableName : phyTableNames) {
                List<String> phyTables = new ArrayList<>();
                phyTables.add(newPhyTableName);
                if(!tableTopology.containsKey(sourceTargetGroup.getValue())) {
                    tableTopology.put(sourceTargetGroup.getValue(), new ArrayList<>());
                }
                tableTopology.get(sourceTargetGroup.getValue())
                    .add(phyTables);
                if(!targetPhyTables.containsKey(sourceTargetGroup.getValue())) {
                    targetPhyTables.put(sourceTargetGroup.getValue(), new HashSet<>());
                }
                targetPhyTables.get(sourceTargetGroup.getValue())
                    .add(newPhyTableName);
                if(!sourcePhyTables.containsKey(sourceTargetGroup.getKey())) {
                    sourcePhyTables.put(sourceTargetGroup.getKey(), new HashSet<>());
                }
                sourcePhyTables.get(sourceTargetGroup.getKey())
                    .add(newPhyTableName);
            }
            if (defaultGroupAndPhyTable == null) {
                defaultGroupAndPhyTable = Pair.of(sourceTargetGroup.getKey(), phyTableNames.get(0));
            }
        }
    }

    @Override
    protected void buildSqlTemplate() {

        Cursor cursor = null;
        try {
            cursor = ExecutorHelper.execute(
                new PhyShow(relDdl.getCluster(),
                    relDdl.getTraitSet(),
                    SqlShowCreateTable
                        .create(SqlParserPos.ZERO,
                            new SqlIdentifier(defaultGroupAndPhyTable.getValue(), SqlParserPos.ZERO)),
                    relDdl.getRowType(),
                    defaultGroupAndPhyTable.getKey(),
                    defaultGroupAndPhyTable.getValue(),
                    preparedData.getSchemaName()), executionContext);

            Row row = null;
            String primaryTableDefinition = null;
            if ((row = cursor.next()) != null) {
                primaryTableDefinition = row.getString(1);
                assert primaryTableDefinition.substring(0, 13).trim().equalsIgnoreCase("CREATE TABLE");
                primaryTableDefinition =
                    primaryTableDefinition.trim().substring(0, 13) + " IF NOT EXISTS " + primaryTableDefinition.trim()
                        .substring(13);
            } else {
                throw new AssertionError(
                    "the table " + defaultGroupAndPhyTable.getValue() + " is not found in " + defaultGroupAndPhyTable
                        .getKey());
            }
            assert primaryTableDefinition != null;
            sqlTemplate = AlterTableGroupUtils.getSqlTemplate(preparedData.getSchemaName(), preparedData.getTableName(),
                primaryTableDefinition, executionContext);
        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }

    }

    public void buildAlterReferenceTableTopology(String schemaName, String tableName) {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        for (Map.Entry<String, ForeignKeyData> fk : tableMeta.getForeignKeys().entrySet()) {
            ForeignKeyData data = fk.getValue();
            if (!data.isPushDown()) {
                continue;
            }
            final List<List<TargetDB>> targetDBs =
                DataNodeChooser.shardChangeTable(data.refSchema, data.refTableName, executionContext);
            if (OptimizerContext.getContext(preparedData.getSchemaName()).getRuleManager()
                .isBroadCast(data.refTableName)) {
                final String phyTableName = targetDBs.get(0).get(0).getTableNames().stream().findFirst().orElse(null);
                assert phyTableName != null;
                for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
                    for (List<String> l : entry.getValue()) {
                        l.add(phyTableName);
                    }
                }
            } else {
                final Map<String, List<List<String>>> refTopo =
                    convertTargetDBs(preparedData.getSchemaName(), targetDBs);

                // push down must be single or broadcast, so only one db
                Map.Entry<String, List<List<String>>> refTable = new ArrayList<>(refTopo.entrySet()).get(0);
                Map.Entry<String, List<List<String>>> table = new ArrayList<>(tableTopology.entrySet()).get(0);
                final List<List<String>> part = refTopo.remove(refTable.getKey());
                refTopo.put(table.getKey(), part);

                assert refTopo.size() == tableTopology.size();
                for (Map.Entry<String, List<List<String>>> entry : refTopo.entrySet()) {
                    final List<List<String>> match = tableTopology.get(entry.getKey());
                    assert match != null;
                    assert match.size() == entry.getValue().size();
                    // Concat one by one.
                    for (int i = 0; i < match.size(); ++i) {
                        match.get(i).addAll(entry.getValue().get(i));
                    }
                }
            }
        }
    }
}
