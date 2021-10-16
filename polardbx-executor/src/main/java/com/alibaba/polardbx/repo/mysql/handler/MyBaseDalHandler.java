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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.BaseDalHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.metadata.InfoSchemaCommon;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.MysqlSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyQueryCursor;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShow;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenmo.cm
 */
public class MyBaseDalHandler extends BaseDalHandler {

    private IRepository myRepo;

    public MyBaseDalHandler(IRepository repo) {
        super(repo);
        myRepo = repo;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;

        // dal should execute on master by default,
        // but if user expect execute it on slave we should not change it to master.
        if (!executionContext.getExtraCmds().containsKey(ConnectionProperties.SLAVE)) {
            executionContext.getExtraCmds().put(ConnectionProperties.MASTER, true);
        }

        try {
            handleForShow(dal, executionContext);
            if (dal.getNativeSqlNode() instanceof SqlShow) {
                try {
                    // MyJdbcHandler does not support show view statement
                    // thus try to handle here
                    Cursor cursor = handleForShowView(dal, executionContext);
                    if (cursor != null) {
                        return cursor;
                    }
                } catch (Throwable t) {
                    // pass
                }
            }
            if (dal.getKind().belongsTo(SqlKind.SQL_SET_QUERY)) {
                if (dal.single()) {
                    MyPhyQueryCursor phyQueryCursor = (MyPhyQueryCursor) myRepo.getCursorFactory()
                        .repoCursor(executionContext, logicalPlan);
                    int[] affectRows = new int[1];
                    affectRows[0] = phyQueryCursor.getAffectedRows();
                    return new AffectRowCursor(affectRows);
                } else {

                    Map<Integer, ParameterContext> params =
                        executionContext.getParams() == null ? null : executionContext.getParams()
                            .getCurrentParameter();
                    List<RelNode> inputs = dal.getInput(params);

                    int affectRow = 0;
                    for (RelNode relNode : inputs) {
                        MyPhyQueryCursor phyQueryCursor = (MyPhyQueryCursor) myRepo.getCursorFactory()
                            .repoCursor(executionContext, relNode);
                        affectRow += phyQueryCursor.getAffectedRows();
                    }
                    int[] affectRows = new int[1];
                    affectRows[0] = affectRow;
                    return new AffectRowCursor(affectRows);
                } // end of else
            } // end of if

            if (dal.single()) {
                return myRepo.getCursorFactory().repoCursor(executionContext, dal.getInput(null).get(0));
            }

            return buildMultiCursor(executionContext, dal);
        } finally {
            // Restore default repo in case we had cross-schema access.
            myRepo = repo;
        }
    }

    private Cursor handleForShowView(BaseDalOperation dal, ExecutionContext executionContext) {

        final SqlShow desc = (SqlShow) dal.getNativeSqlNode();
        final String tableName = RelUtils.lastStringValue(desc.getTableName());
        String schemaName = executionContext.getSchemaName();
        ViewManager viewManager;
        if (InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
            viewManager = InformationSchemaViewManager.getInstance();
        } else if (RelUtils.informationSchema(desc.getTableName())) {
            viewManager = InformationSchemaViewManager.getInstance();
        } else if (RelUtils.mysqlSchema(desc.getTableName())) {
            viewManager = MysqlSchemaViewManager.getInstance();
        } else {
            viewManager = OptimizerContext.getContext(schemaName).getViewManager();
        }

        SystemTableView.Row row = viewManager.select(tableName);
        if (row != null) {
            RelDataType rowType;
            if (row.isVirtual()) {
                VirtualViewType virtualViewType = row.getVirtualViewType();
                rowType = VirtualView
                    .create(
                        SqlConverter.getInstance(executionContext).createRelOptCluster(PlannerContext.EMPTY_CONTEXT),
                        virtualViewType).getRowType();
            } else {
                SqlNode ast = new FastsqlParser().parse(row.getViewDefinition())
                    .get(0);
                SqlConverter converter = SqlConverter.getInstance(executionContext);
                SqlNode validatedNode = converter.validate(ast);
                rowType = converter.toRel(validatedNode).getRowType();
            }

            ArrayResultCursor resultCursor = new ArrayResultCursor(tableName);
//                    | Field | Type    | Null | Key | Default | Extra |
            resultCursor.addColumn("Field", DataTypes.StringType);
            resultCursor.addColumn("Type", DataTypes.StringType);
            resultCursor.addColumn("Null", DataTypes.StringType);
            resultCursor.addColumn("Key", DataTypes.StringType);
            resultCursor.addColumn("Default", DataTypes.StringType);
            resultCursor.addColumn("Extra", DataTypes.StringType);

            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String field = rowType.getFieldList().get(i).getName();
                RelDataType type = rowType.getFieldList().get(i).getType();
                if (row.getColumnList() != null) {
                    field = row.getColumnList().get(i);
                }
                // todo 此处LogicalDescHandler实现中Null字段默认为YES, 且Type存在问题
                resultCursor.addRow(new Object[] {field, type.toString().toLowerCase(), "YES", "", "NULL", ""});
            }
            return resultCursor;
        }
        // not view
        return null;
    }

    private void handleForShow(BaseDalOperation dal, ExecutionContext executionContext) {
        if (dal.getNativeSqlNode() instanceof SqlShow) {
            SqlNode dbNameNode = ((SqlShow) dal.getNativeSqlNode()).getDbName();

            if (dbNameNode != null && dbNameNode instanceof SqlIdentifier) {
                String targetSchema = ((SqlIdentifier) dbNameNode).getLastName();

                if (TStringUtil.isNotBlank(targetSchema)
                    && !InfoSchemaCommon.MYSQL_SYS_SCHEMAS.contains(targetSchema.toUpperCase())
                    && !TStringUtil.equalsIgnoreCase(targetSchema, executionContext.getSchemaName())) {

                    // Find target optimizer and executor contexts.
                    OptimizerContext optimizerContext = OptimizerContext.getContext(targetSchema);
                    ExecutorContext executorContext = ExecutorContext.getContext(targetSchema);
                    if (optimizerContext == null || executorContext == null) {
                        GeneralUtil
                            .nestedException("Cannot find schema: " + targetSchema + ", please check your sql again.");
                    }

                    // Replace target repository
                    myRepo = executorContext.getRepositoryHolder().get(GroupType.MYSQL_JDBC.name());

                    // Replace target db index and physical table name.
                    String logicalTableName = ((SqlShow) dal.getNativeSqlNode()).getTableName().toString();

                    if (optimizerContext.getPartitionInfoManager().isNewPartDbTable(logicalTableName)) {
                        PhysicalPartitionInfo prunedPartInfo =
                            optimizerContext.getPartitionInfoManager().getFirstPhysicalPartition(logicalTableName);
                        String dbIndex = prunedPartInfo.getGroupKey();
                        String phyTb = prunedPartInfo.getPhyTable();
                        dal.setDbIndex(dbIndex);
                        dal.setPhyTable(phyTb);
                    } else {
                        final TargetDB targetDB = optimizerContext.getRuleManager().shardAny(logicalTableName);
                        dal.setDbIndex(targetDB.getDbIndex());
                        String targetPhyTable = getTargetPhyTable(logicalTableName, optimizerContext);
                        if (TStringUtil.isNotEmpty(targetPhyTable)) {
                            dal.setPhyTable(targetPhyTable);
                        }
                    }

                }
            }
        }
    }

    private String getTargetPhyTable(String logicalTableName, OptimizerContext optimizerContext) {
        String targetPhyTable = null;

        final TargetDB targetDB = optimizerContext.getRuleManager().shardAny(logicalTableName);
        final String defaultDbIndex = targetDB.getDbIndex();
        TableRule tableRule = optimizerContext.getRuleManager().getTableRule(logicalTableName);

        if (tableRule != null && tableRule.getActualTopology() != null
            && tableRule.getActualTopology().values().size() > 0) {
            for (Map.Entry entry : tableRule.getActualTopology().entrySet()) {
                if (TStringUtil.equalsIgnoreCase((String) entry.getKey(), defaultDbIndex)) {
                    Set<String> physicalTablesInDefaultDb = (Set<String>) entry.getValue();
                    if (physicalTablesInDefaultDb != null && physicalTablesInDefaultDb.size() > 0) {
                        targetPhyTable = physicalTablesInDefaultDb.iterator().next();
                    }
                    break;
                }
            }
        }

        return targetPhyTable;
    }

}
