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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.repo.mysql.common.ResultSetHelper;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * @author mengshi
 */
public class LogicalShowCreateTableHandler extends HandlerCommon {

    private final LogicalShowCreateTablesForPartitionDatabaseHandler partitionDatabaseHandler;
    private final LogicalShowCreateTablesForShardingDatabaseHandler shardingDatabaseHandler;

    public LogicalShowCreateTableHandler(IRepository repo) {
        super(repo);
        partitionDatabaseHandler = new LogicalShowCreateTablesForPartitionDatabaseHandler(repo);
        shardingDatabaseHandler = new LogicalShowCreateTablesForShardingDatabaseHandler(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;

        String schemaName = show.getSchemaName();

        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return partitionDatabaseHandler.handle(logicalPlan, executionContext);
        } else {
            return shardingDatabaseHandler.handle(logicalPlan, executionContext);
        }
    }

    public static String reorgLogicalColumnOrder(String schemaName, String logicalTableName, String sql) {
        // Always show columns in logical column order no matter what the mode.
        List<ColumnsRecord> logicalColumnsInOrder =
            ResultSetHelper.fetchLogicalColumnsInOrder(schemaName, logicalTableName);

        if (GeneralUtil.isEmpty(logicalColumnsInOrder)) {
            return sql;
        }

        List<SQLTableElement> newTableElements = new ArrayList<>();

        final MySqlCreateTableStatement createTableStmt =
            (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(sql, JdbcConstants.MYSQL).get(0)
                .clone();

        for (SQLTableElement tableElement : createTableStmt.getTableElementList()) {
            if (tableElement instanceof MysqlForeignKey) {
                if (((MysqlForeignKey) tableElement).getName() != null) {
                    ((MysqlForeignKey) tableElement).setHasConstraint(true);
                }
            }
        }

        for (ColumnsRecord logicalColumn : logicalColumnsInOrder) {
            for (SQLTableElement tableElement : createTableStmt.getTableElementList()) {
                if (tableElement instanceof SQLColumnDefinition) {
                    String physicalColumnName =
                        SQLUtils.normalize(((SQLColumnDefinition) tableElement).getColumnName());
                    if (TStringUtil.equalsIgnoreCase(physicalColumnName, logicalColumn.columnName)) {
                        newTableElements.add(tableElement);
                        break;
                    }
                }
            }
        }

        for (SQLTableElement tableElement : createTableStmt.getTableElementList()) {
            if (!(tableElement instanceof SQLColumnDefinition)) {
                newTableElements.add(tableElement);
            }
        }

        createTableStmt.getTableElementList().clear();
        createTableStmt.getTableElementList().addAll(newTableElements);

        return createTableStmt.toString();
    }

}
