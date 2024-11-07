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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.privilege.MySQLPrivilegesName;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.BaseDalHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaCollationsHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.DBStatusRecord;
import com.alibaba.polardbx.gms.metadb.table.EnginesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
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
import com.alibaba.polardbx.repo.mysql.common.ResultSetHelper;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyQueryCursor;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShow;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenmo.cm
 */
public class MyBaseDalHandler extends BaseDalHandler {

    public MyBaseDalHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;

        // dal should execute on master by default,
        // but if user expect execute it on slave we should not change it to master.
        if (!executionContext.getExtraCmds().containsKey(ConnectionProperties.SLAVE)) {
            executionContext.getExtraCmds().put(ConnectionProperties.MASTER, true);
        }
        if (ConfigDataMode.isColumnarMode() || executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LOGICAL_TABLE_META)) {
            Cursor cursor = handleInColumnarMode(logicalPlan, executionContext);
//            if cursor == null, use the original path
            if (cursor != null) {
                return cursor;
            }
        }

        IRepository myRepo = handleForShow(dal, executionContext);
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
            ShowColumnsContext showColumnsContext = extractSchemaTableNameForShowColumns(dal, executionContext);
            Cursor cursor = myRepo.getCursorFactory().repoCursor(executionContext, dal.getInput(null).get(0));
            return reorgLogicalColumnOrder(showColumnsContext, cursor, executionContext);
        }

        return buildMultiCursor(executionContext, dal);
    }

    private Cursor handleInColumnarMode(RelNode logicalPlan, ExecutionContext ec) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;

        if (dal.getKind() == SqlKind.SHOW && dal.getNativeSqlNode() instanceof SqlShow) {
            if (TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW COLUMNS") ||
                TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW FULL COLUMNS")) {
                return handleShowColumnInColumnarMode(logicalPlan, ec);
            } else if (TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW COLLATION")) {
                return handleShowCollationInColumnarMode(logicalPlan, ec);
            } else if (TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW CHARACTER SET")) {
                return handleShowCharacterSetInColumnarMode(logicalPlan, ec);
            } else if (TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW PRIVILEGES")) {
                return handleShowPrivilegesInColumnarMode(logicalPlan, ec);
            } else if (TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW ENGINES")) {
                return handleShowEnginesInColumnarMode(logicalPlan, ec);
            } else if (TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW STATUS")) {
                return handleShowStatusInColumnarMode(logicalPlan, ec);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private Cursor handleShowStatusInColumnarMode(RelNode logicalPlan, ExecutionContext ec) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;
        String schemaName = dal.getSchemaName();
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        String sql = "show status";
        List<DBStatusRecord> dbStatusRecords;
        try (Connection connection = dataSource.getConnection()) {
            dbStatusRecords = MetaDbUtil.query(sql, DBStatusRecord.class, connection);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                "fail to access metadb" + e.getMessage());
        }
        ArrayResultCursor resultCursor = new ArrayResultCursor(schemaName);
        resultCursor.addColumn("Variable_name", DataTypes.StringType, false);
        resultCursor.addColumn("Value", DataTypes.StringType, false);
        for (DBStatusRecord dbStatusRecord : dbStatusRecords) {
            resultCursor.addRow(new Object[] {
                dbStatusRecord.variableName, dbStatusRecord.value
            });
        }
        return resultCursor;
    }
    private Cursor handleShowEnginesInColumnarMode(RelNode logicalPlan, ExecutionContext ec) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;
        String schemaName = dal.getSchemaName();
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        String sql = "show engines";
        List<EnginesRecord> enginesRecords;
        try (Connection connection = dataSource.getConnection()) {
            enginesRecords = MetaDbUtil.query(sql, EnginesRecord.class, connection);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                "fail to access metadb" + e.getMessage());
        }
        ArrayResultCursor resultCursor = new ArrayResultCursor(schemaName);
        resultCursor.addColumn("Engine", DataTypes.StringType, false);
        resultCursor.addColumn("Support", DataTypes.StringType, false);
        resultCursor.addColumn("Comment", DataTypes.StringType, false);
        resultCursor.addColumn("Transaction", DataTypes.StringType, false);
        resultCursor.addColumn("XA", DataTypes.StringType, false);
        resultCursor.addColumn("Savepoints", DataTypes.StringType, false);
        for (EnginesRecord record : enginesRecords) {
            resultCursor.addRow(new Object[] {
                record.engine, record.support, record.comment, record.transcations, record.XA, record.savePoints
            });
        }
        return resultCursor;
    }


    private Cursor handleShowColumnInColumnarMode(RelNode logicalPlan, ExecutionContext ec) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        ShowColumnsContext showColumnsContext = extractSchemaTableNameForShowColumns(dal, ec);
        Assert.assertTrue(showColumnsContext != null, "showColumnsContext shouldn't be null");
        String sql = "select * from " + SqlIdentifier.surroundWithBacktick(GmsSystemTables.COLUMNS)
            + " where table_schema = ? and table_name = ? and column_name != ?";
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, showColumnsContext.schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, showColumnsContext.tableName);
        //do not show _drds_implicit_id_ by default
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, "_drds_implicit_id_");
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords;
        try (Connection connection = dataSource.getConnection()) {
            columnsInfoSchemaRecords = MetaDbUtil.query(sql, params, ColumnsInfoSchemaRecord.class, connection);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                "fail to access metadb" + e.getMessage());
        }
        ArrayResultCursor resultCursor = new ArrayResultCursor(showColumnsContext.tableName);
        if (showColumnsContext != null) {
            resultCursor.addColumn("Field", DataTypes.StringType, false);
            resultCursor.addColumn("Type", DataTypes.StringType, false);
            if (showColumnsContext.isFull) {
                resultCursor.addColumn("Collation", DataTypes.StringType, false);
            }
            resultCursor.addColumn("Null", DataTypes.StringType, false);
            resultCursor.addColumn("Key", DataTypes.StringType, false);
            resultCursor.addColumn("Default", DataTypes.StringType, false);
            resultCursor.addColumn("Extra", DataTypes.StringType, false);
            if (showColumnsContext.isFull) {
                resultCursor.addColumn("Privileges", DataTypes.StringType, false);
                resultCursor.addColumn("Comment", DataTypes.StringType, false);
            }
        }
        for (ColumnsInfoSchemaRecord record : columnsInfoSchemaRecords) {
            if (showColumnsContext.isFull) {
                resultCursor.addRow(new Object[] {
                    record.columnName, record.columnType.toLowerCase(), record.collationName, record.isNullable,
                    record.columnKey, record.columnDefault, record.extra, record.privileges,
                    record.columnComment
                });
            } else {
                resultCursor.addRow(new Object[] {
                    record.columnName, record.columnType.toLowerCase(), record.isNullable, record.columnKey,
                    record.columnDefault,
                    record.extra
                });
            }
        }
        return resultCursor;
    }

    //information_schema.collation is a virtual view, so we should not access metadb
    private Cursor handleShowCollationInColumnarMode(RelNode logicalPlan, ExecutionContext ec) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;
        String schemaName = dal.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = ec.getSchemaName();
        }
        ArrayResultCursor resultCursor = new ArrayResultCursor(schemaName);
        resultCursor.addColumn("Collation", DataTypes.StringType, false);
        resultCursor.addColumn("Charset", DataTypes.StringType, false);
        resultCursor.addColumn("Id", DataTypes.LongType, false);
        resultCursor.addColumn("Default", DataTypes.StringType, false);
        resultCursor.addColumn("Compiled", DataTypes.StringType, false);
        resultCursor.addColumn("Sortlen", DataTypes.LongType, false);
        for (Object[] record : InformationSchemaCollationsHandler.COLLATIONS) {
            //PADAttribute columns will be ignored automatically
            resultCursor.addRow(record);
        }
        return resultCursor;
    }

    private Cursor handleShowCharacterSetInColumnarMode(RelNode logicalPlan, ExecutionContext ec) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;
        String schemaName = dal.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = ec.getSchemaName();
        }
        ArrayResultCursor resultCursor = new ArrayResultCursor(schemaName);
        resultCursor.addColumn("Charset", DataTypes.StringType, false);
        resultCursor.addColumn("Description", DataTypes.StringType, false);
        resultCursor.addColumn("Default collation", DataTypes.StringType, false);
        resultCursor.addColumn("Maxlen", DataTypes.LongType, false);
        for (CharsetName record : CharsetName.values()) {
            if (record != null) {
                resultCursor.addRow(new Object[] {
                    record.name(),
                    //since we do not record description, this field will be temporarily null
                    "",
                    record.getDefaultCollationName(),
                    record.getMaxLen()
                });
            }
        }
        return resultCursor;
    }

    private Cursor handleShowPrivilegesInColumnarMode(RelNode logicalNode, ExecutionContext ec) {
        final BaseDalOperation dal = (BaseDalOperation) logicalNode;
        String schemaName = dal.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = ec.getSchemaName();
        }
        ArrayResultCursor resultCursor = new ArrayResultCursor(schemaName);
        resultCursor.addColumn("Privilege", DataTypes.StringType, false);
        resultCursor.addColumn("Context", DataTypes.StringType, false);
        resultCursor.addColumn("Comment", DataTypes.StringType, false);
        for (MySQLPrivilegesName record : MySQLPrivilegesName.values()) {
            if (record != null) {
                resultCursor.addRow(new Object[] {
                    record.getPrivilege(),
                    record.getContext(),
                    record.getComment()
                });
            }
        }
        return resultCursor;
    }

    private Cursor reorgLogicalColumnOrder(ShowColumnsContext context, Cursor cursor, ExecutionContext ec) {
        if (context != null) {
            ArrayResultCursor resultCursor = new ArrayResultCursor(context.tableName);

            resultCursor.addColumn("Field", DataTypes.StringType, false);
            resultCursor.addColumn("Type", DataTypes.StringType, false);
            if (context.isFull) {
                resultCursor.addColumn("Collation", DataTypes.StringType, false);
            }
            resultCursor.addColumn("Null", DataTypes.StringType, false);
            resultCursor.addColumn("Key", DataTypes.StringType, false);
            resultCursor.addColumn("Default", DataTypes.StringType, false);
            resultCursor.addColumn("Extra", DataTypes.StringType, false);
            if (context.isFull) {
                resultCursor.addColumn("Privileges", DataTypes.StringType, false);
                resultCursor.addColumn("Comment", DataTypes.StringType, false);
            }

            resultCursor.initMeta();

            List<Object[]> rows = new ArrayList<>();
            try {
                Row row;
                while ((row = cursor.next()) != null) {
                    if (context.isFull) {
                        rows.add(new Object[] {
                            row.getString(0),
                            row.getString(1),
                            row.getString(2),
                            row.getString(3),
                            row.getString(4),
                            row.getString(5),
                            row.getString(6),
                            row.getString(7),
                            row.getString(8)
                        });
                    } else {
                        rows.add(new Object[] {
                            row.getString(0),
                            row.getString(1),
                            row.getString(2),
                            row.getString(3),
                            row.getString(4),
                            row.getString(5)
                        });
                    }
                }
            } finally {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            }
            rows = ResultSetHelper.processColumnInfos(context.schemaName, context.tableName, rows, ec);
            ResultSetHelper.reorgLogicalColumnOrder(context.schemaName, context.tableName, rows, resultCursor);

            return resultCursor;
        }

        return cursor;
    }

    private ShowColumnsContext extractSchemaTableNameForShowColumns(BaseDalOperation dal,
                                                                    ExecutionContext executionContext) {
        if (dal.getKind() == SqlKind.SHOW && dal.getNativeSqlNode() instanceof SqlShow) {
            boolean isShowColumns = TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW COLUMNS");
            boolean isShowFullColumns = TStringUtil.startsWithIgnoreCase(dal.getNativeSql(), "SHOW FULL COLUMNS");
            if (isShowColumns || isShowFullColumns) {
                SqlShow sqlShow = (SqlShow) dal.getNativeSqlNode();

                String schemaName = null;
                if (sqlShow.getDbName() != null) {
                    schemaName = ((SqlIdentifier) sqlShow.getDbName()).getLastName();
                }
                if (TStringUtil.isEmpty(schemaName)) {
                    schemaName = dal.getSchemaName();
                }
                if (TStringUtil.isEmpty(schemaName)) {
                    schemaName = executionContext.getSchemaName();
                }

                String tableName = ((SqlIdentifier) sqlShow.getTableName()).getLastName();

                ShowColumnsContext context = new ShowColumnsContext();
                context.isFull = isShowFullColumns;
                context.schemaName = schemaName;
                context.tableName = tableName;

                return context;
            }
        }
        return null;
    }

    private class ShowColumnsContext {
        public boolean isFull;
        public String schemaName;
        public String tableName;
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

            ShowColumnsContext showColumnsContext = extractSchemaTableNameForShowColumns(dal, executionContext);
            ArrayResultCursor resultCursor = new ArrayResultCursor(tableName);
            if (showColumnsContext != null) {
                resultCursor.addColumn("Field", DataTypes.StringType, false);
                resultCursor.addColumn("Type", DataTypes.StringType, false);
                if (showColumnsContext.isFull) {
                    resultCursor.addColumn("Collation", DataTypes.StringType, false);
                }
                resultCursor.addColumn("Null", DataTypes.StringType, false);
                resultCursor.addColumn("Key", DataTypes.StringType, false);
                resultCursor.addColumn("Default", DataTypes.StringType, false);
                resultCursor.addColumn("Extra", DataTypes.StringType, false);
                if (showColumnsContext.isFull) {
                    resultCursor.addColumn("Privileges", DataTypes.StringType, false);
                    resultCursor.addColumn("Comment", DataTypes.StringType, false);
                }
            }

            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String field = rowType.getFieldList().get(i).getName();
                RelDataType type = rowType.getFieldList().get(i).getType();
                if (row.getColumnList() != null) {
                    field = row.getColumnList().get(i);
                }
                // todo 此处LogicalDescHandler实现中Null字段默认为YES, 且Type存在问题
                if (showColumnsContext.isFull) {
                    resultCursor.addRow(new Object[] {field, type.toString().toLowerCase(), "NULL", "YES", "", "NULL", "", "select", ""});
                } else {
                    resultCursor.addRow(new Object[] {field, type.toString().toLowerCase(), "YES", "", "NULL", ""});
                }
            }
            return resultCursor;
        }
        // not view
        return null;
    }

    private IRepository handleForShow(BaseDalOperation dal, ExecutionContext executionContext) {
        IRepository myRepo = repo;
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
        return myRepo;
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
