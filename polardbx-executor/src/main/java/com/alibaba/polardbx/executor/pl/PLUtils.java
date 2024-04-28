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

package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.optimizer.core.datatype.Blob;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.util.SpParameter;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsSpParameterizeSqlVisitor;
import io.airlift.slice.Slice;
import org.apache.calcite.util.BitString;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PLUtils {
    public static int INTERNAL_CACHE_SIZE = 10;

    /**
     * replace pl variables in sql
     */
    public static SpParameterizedStmt parameterize(Map<String, SpParameter> spVariables, String sql) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcConstants.MYSQL,
            SQLUtils.parserFeatures);
        List<SQLStatement> statements = parser.parseStatementList();
        final SQLStatement statement = statements.get(0);
        return parameterize(spVariables, statement);
    }

    public static SpParameterizedStmt parameterize(Map<String, SpParameter> spVariables, SQLStatement stmt) {
        StringBuilder out = new StringBuilder();
        DrdsSpParameterizeSqlVisitor visitor = new DrdsSpParameterizeSqlVisitor(out, false, spVariables);
        stmt.accept(visitor);
        return new SpParameterizedStmt(out.toString(), visitor.getSpParameters());
    }

    public static boolean convertObjectToBoolean(Object val) {
        if (val == null) {
            return false;
        }
        return DataTypes.BooleanType.convertFrom(val) > 0;
    }

    public static String getPrintString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Decimal) {
            value = ((Decimal) value).toBigDecimal();
        }
        if (value instanceof Number) {
            return ((Number) value).toString();
        }
        if (value instanceof byte[]) {
            return "0x" + BitString.createFromBytes((byte[]) value).toHexString();
        } else if (value instanceof com.alibaba.polardbx.optimizer.core.datatype.Blob) {
            try {
                return "0x" + BitString.createFromBytes(((Blob) value).getBytes(1, (int) ((Blob) value).length()))
                    .toHexString();
            } catch (SQLException e) {
                throw new RuntimeException("get blob data failed!");
            }
        }
        String printStr;
        if (value instanceof Slice) {
            printStr = ((Slice) value).toStringUtf8();
        } else if (value instanceof EnumValue) {
            printStr = ((EnumValue) value).getValue();
        } else {
            printStr = value.toString();
        }
        return "\"" + printStr + "\"";
    }

    private static MemoryPool createCursorMemoryPool(PlContext procContext, MemoryPool parentMemoryPool) {
        String memoryName = "PL_CURSOR_" + procContext.getNextCursorId();
        MemoryPool pool = parentMemoryPool.getOrCreatePool(
            memoryName, procContext.getCursorMemoryLimit(), MemoryType.PROCEDURE_CURSOR);
        return pool;
    }

    /**
     * Stash all result row for further use, and close the original cursor
     */
    public static PlCacheCursor buildCacheCursor(Cursor cursor, PlContext procContext) {
        PlCacheCursor cacheCursor = null;
        try {
            MemoryPool pool = createCursorMemoryPool(procContext, procContext.getCurrentMemoryPool());
            long estimateRowSize = cursor.getReturnColumns().stream().mapToLong(
                MemoryEstimator::estimateColumnSize).sum();
            cacheCursor = new PlCacheCursor(ServiceProvider.getInstance().getServer().getSpillerFactory(),
                cursor, pool, estimateRowSize, procContext.getSpillMonitor());
            cacheCursor.cacheAllRows();
            return cacheCursor;
        } catch (Exception ex) {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
            if (cacheCursor != null) {
                cacheCursor.close(new ArrayList<>());
            }
            throw new RuntimeException(ex.getMessage());
        }
    }

    public static String getCurrentTime() {
        long time = System.currentTimeMillis();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return formatter.format(time);
    }

    public static String getProcedureSchema(SQLName name, String executionSchema) {
        String procedureSchema = executionSchema;
        if (name instanceof SQLPropertyExpr) {
            procedureSchema = SQLUtils.normalize(((SQLPropertyExpr) name).getOwnerName());
        }
        return procedureSchema;
    }

    public static String getCreateFunctionOnDn(String content) {
        SQLCreateFunctionStatement statement = (SQLCreateFunctionStatement) FastsqlUtils.parseSql(content).get(0);
        statement.setComment(new SQLCharExpr(PlConstants.POLARX_COMMENT));
        return statement.toString();
    }
}

