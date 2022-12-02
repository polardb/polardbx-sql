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

package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.druid.sql.SQLUtils.normalize;

/**
 * Sql Utils for Cdc moudle
 * Created by ziyang.lb
 **/
public class SQLHelper {
    public final static SQLParserFeature[] FEATURES = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
    };

    static String getSqlName(SQLExpr sqlName) {
        if (sqlName == null) {
            return null;
        }

        if (sqlName instanceof SQLPropertyExpr) {
            SQLIdentifierExpr owner = (SQLIdentifierExpr) ((SQLPropertyExpr) sqlName).getOwner();
            return SQLUtils.normalize(owner.getName()) + "."
                + SQLUtils.normalize(((SQLPropertyExpr) sqlName).getName());
        } else if (sqlName instanceof SQLIdentifierExpr) {
            return SQLUtils.normalize(((SQLIdentifierExpr) sqlName).getName());
        } else if (sqlName instanceof SQLCharExpr) {
            return ((SQLCharExpr) sqlName).getText();
        } else if (sqlName instanceof SQLMethodInvokeExpr) {
            return SQLUtils.normalize(((SQLMethodInvokeExpr) sqlName).getMethodName());
        } else if (sqlName instanceof MySqlOrderingExpr) {
            return getSqlName(((MySqlOrderingExpr) sqlName).getExpr());
        } else {
            return sqlName.toString();
        }
    }

    static void filterColumns(MySqlCreateTableStatement stmt, String schema, String tableName)
        throws SQLException {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnsAccessor accessor = new ColumnsAccessor();
            accessor.setConnection(metaDbConn);
            List<ColumnsRecord> columnsRecords = accessor.query(schema, tableName);

            List<String> toRemoveColumns = columnsRecords.stream()
                .filter(c -> c.status == ColumnStatus.MULTI_WRITE_TARGET.getValue())
                .map(c -> {
                    c.columnName = StringUtils.lowerCase(c.columnName);
                    return c.columnName;
                }).collect(Collectors.toList());

            filterColumns(stmt, toRemoveColumns);
        }
    }

    static void filterColumns(MySqlCreateTableStatement stmt, List<String> toRemoveColumns) {
        if (!toRemoveColumns.isEmpty()) {
            Iterator<SQLTableElement> iterator = stmt.getTableElementList().iterator();
            while (iterator.hasNext()) {
                SQLTableElement element = iterator.next();
                if (element instanceof SQLColumnDefinition) {
                    SQLColumnDefinition definition = (SQLColumnDefinition) element;
                    String c1 = normalize(definition.getColumnName());
                    if (toRemoveColumns.contains(c1.toLowerCase())) {
                        iterator.remove();
                    }
                } else if (element instanceof MySqlPrimaryKey) {
                    MySqlPrimaryKey column = (MySqlPrimaryKey) element;
                    List<SQLSelectOrderByItem> pks = column.getColumns();
                    for (SQLSelectOrderByItem pk : pks) {
                        String name = getSqlName(pk.getExpr());
                        if (toRemoveColumns.contains(name.toLowerCase())) {
                            iterator.remove();
                            break;
                        }
                    }
                } else if (element instanceof MySqlUnique) {
                    MySqlUnique column = (MySqlUnique) element;
                    List<SQLSelectOrderByItem> uks = column.getColumns();
                    for (SQLSelectOrderByItem uk : uks) {
                        String name = getSqlName(uk.getExpr());
                        if (toRemoveColumns.contains(name.toLowerCase())) {
                            iterator.remove();
                            break;
                        }
                    }
                } else if (element instanceof MySqlTableIndex) {
                    MySqlTableIndex column = (MySqlTableIndex) element;
                    List<SQLSelectOrderByItem> indexes = column.getColumns();
                    for (SQLSelectOrderByItem idx : indexes) {
                        String name = getSqlName(idx.getExpr());
                        if (toRemoveColumns.contains(name.toLowerCase())) {
                            iterator.remove();
                            break;
                        }
                    }
                }
            }
        }
    }
}
