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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.MysqlSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import com.alibaba.polardbx.repo.mysql.common.ResultSetHelper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDesc;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class LogicalDescHandler extends HandlerCommon {

    public LogicalDescHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlDesc desc = (SqlDesc) show.getNativeSqlNode();
        final String tableName = RelUtils.lastStringValue(desc.getTableName());

        String schemaName = null;
        if (desc.getDbName() != null) {
            schemaName = ((SqlIdentifier) desc.getDbName()).getLastName();
        }
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = show.getSchemaName();
        }
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        try {
            TableMeta tableMeta =
                OptimizerContext.getContext(executionContext.getSchemaName()).getLatestSchemaManager()
                    .getTable(tableName);
            if (tableMeta.getStatus() != TableStatus.PUBLIC) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, tableName);
            }
        } catch (Throwable t1) {
            // can not find tableMeta, so try view
            try {

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
                                SqlConverter.getInstance(schemaName, executionContext)
                                    .createRelOptCluster(PlannerContext.EMPTY_CONTEXT),
                                virtualViewType).getRowType();
                    } else {
                        SqlNode ast = new FastsqlParser().parse(row.getViewDefinition()).get(0);
                        SqlConverter converter = SqlConverter.getInstance(schemaName, executionContext);
                        SqlNode validatedNode = converter.validate(ast);
                        rowType = converter.toRel(validatedNode).getRowType();
                    }

                    ArrayResultCursor resultCursor = new ArrayResultCursor(tableName);
//                    | Field | Type    | Null | Key | Default | Extra |
                    resultCursor.addColumn("Field", DataTypes.StringType, false);
                    resultCursor.addColumn("Type", DataTypes.StringType, false);
                    resultCursor.addColumn("Null", DataTypes.StringType, false);
                    resultCursor.addColumn("Key", DataTypes.StringType, false);
                    resultCursor.addColumn("Default", DataTypes.StringType, false);
                    resultCursor.addColumn("Extra", DataTypes.StringType, false);

                    for (int i = 0; i < rowType.getFieldCount(); i++) {
                        String field = rowType.getFieldList().get(i).getName();
                        RelDataType type = rowType.getFieldList().get(i).getType();
                        if (row.getColumnList() != null) {
                            field = row.getColumnList().get(i);
                        }
                        resultCursor.addRow(new Object[] {field, type.toString().toLowerCase(), "YES", "", "NULL", ""});
                    }
                    return resultCursor;
                }
            } catch (Throwable t2) {
                // pass
            }
        }

        PhyShow phyShow = new PhyShow(show.getCluster(),
            show.getTraitSet(),
            desc,
            show.getRowType(),
            show.getDbIndex(),
            show.getPhyTable(),
            show.getSchemaName());
        String descTableName = TStringUtil.isNotBlank(show.getPhyTable()) ? show.getPhyTable() : tableName;
        descTableName = TStringUtil.addBacktick(descTableName);
        if (RelUtils.informationSchema(desc.getTableName())) {
            descTableName = "information_schema." + descTableName;
        } else if (RelUtils.mysqlSchema(desc.getTableName())) {
            descTableName = "mysql." + descTableName;
        }
        phyShow.setSqlTemplate("DESC " + descTableName);

        Cursor cursor = repo.getCursorFactory().repoCursor(executionContext, phyShow);
        return reorgLogicalColumnOrder(schemaName, tableName, cursor);
    }

    private Cursor reorgLogicalColumnOrder(String schemaName, String tableName, Cursor cursor) {
        ArrayResultCursor resultCursor = new ArrayResultCursor(tableName);

        resultCursor.addColumn("Field", DataTypes.StringType);
        resultCursor.addColumn("Type", DataTypes.StringType);
        resultCursor.addColumn("Null", DataTypes.StringType);
        resultCursor.addColumn("Key", DataTypes.StringType);
        resultCursor.addColumn("Default", DataTypes.StringType);
        resultCursor.addColumn("Extra", DataTypes.StringType);

        resultCursor.initMeta();

        List<Object[]> rows = new ArrayList<>();
        try {
            Row row;
            while ((row = cursor.next()) != null) {
                rows.add(new Object[] {
                    row.getString(0),
                    row.getString(1),
                    row.getString(2),
                    row.getString(3),
                    row.getString(4),
                    row.getString(5)
                });
            }
        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }

        ResultSetHelper.reorgLogicalColumnOrder(schemaName, tableName, rows, resultCursor);

        return resultCursor;
    }

}
