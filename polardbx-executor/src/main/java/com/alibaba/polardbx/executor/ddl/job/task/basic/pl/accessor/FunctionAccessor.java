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

package com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.pl.function.FunctionDefinitionRecord;
import com.alibaba.polardbx.gms.metadb.pl.function.FunctionMetaRecord;
import com.alibaba.polardbx.gms.metadb.pl.procedure.CreateProcedureRecord;
import com.alibaba.polardbx.gms.metadb.pl.procedure.ProcedureStatusRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.DEF_ROUTINE_CATALOG;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.FUNCTION;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.MOCK_CHARACTER_SET_CLIENT;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.MOCK_COLLATION_CONNECTION;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.MOCK_DATABASE_COLLATION;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.MYSQL;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.NO_SQL;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.SQL;

public class FunctionAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(FunctionAccessor.class);

    // TODO CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION are fake
    static final String INSERT_FUNCTION = String.format("insert into %s" +
            " (SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, "
            + "DATA_TYPE, ROUTINE_BODY, ROUTINE_DEFINITION, PARAMETER_STYLE, IS_DETERMINISTIC"
            + ", SQL_DATA_ACCESS, SECURITY_TYPE, CREATED, LAST_ALTERED, SQL_MODE, ROUTINE_COMMENT"
            + ", DEFINER, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION, ROUTINE_META) "
            + "values ( ?, '%s', '%s', ?, '%s', ? , '%s', ?, '%s', ?, ?, "
            + " ?, ?, ?, ?, ?, ?, '%s', '%s', '%s', ?);", GmsSystemTables.ROUTINES, DEF_ROUTINE_CATALOG, MYSQL, FUNCTION,
        SQL,
        SQL, MOCK_CHARACTER_SET_CLIENT, MOCK_COLLATION_CONNECTION, MOCK_DATABASE_COLLATION);

    static final String DROP_FUNCTION =
        String.format("delete from %s where ROUTINE_NAME = ? and ROUTINE_TYPE = '%s'", GmsSystemTables.ROUTINES,
            FUNCTION);

    static final String LOAD_FUNCTION_METAS =
        String.format("SELECT ROUTINE_NAME, ROUTINE_META, SQL_DATA_ACCESS FROM routines WHERE ROUTINE_TYPE = '%s'",
            FUNCTION);

    private final static String SHOW_FUNCTION_STATUS =
        String.format("SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, "
                + "DEFINER, LAST_ALTERED, CREATED, SECURITY_TYPE, ROUTINE_COMMENT, CHARACTER_SET_CLIENT, "
                + "COLLATION_CONNECTION, DATABASE_COLLATION FROM %s WHERE ROUTINE_NAME LIKE ? AND ROUTINE_TYPE = '%s'",
            GmsSystemTables.ROUTINES, FUNCTION);

    private final static String FIND_FUNCTION_DEFINITION =
        String.format(
            "SELECT ROUTINE_META, SQL_DATA_ACCESS, ROUTINE_NAME, ROUTINE_DEFINITION FROM routines WHERE ROUTINE_NAME = ? AND ROUTINE_TYPE = '%s'",
            FUNCTION);

    private final static String GET_PUSHABLE_FUNCTIONS =
        String.format(
            "SELECT ROUTINE_META, SQL_DATA_ACCESS, ROUTINE_NAME, ROUTINE_DEFINITION FROM routines WHERE ROUTINE_TYPE = '%s' AND SQL_DATA_ACCESS = '%s'",
            FUNCTION, NO_SQL);

    private static String SHOW_CREATE_FUNCTION =
        String.format("SELECT ROUTINE_SCHEMA, ROUTINE_NAME, SQL_MODE, ROUTINE_DEFINITION, CHARACTER_SET_CLIENT, "
                + "COLLATION_CONNECTION, DATABASE_COLLATION FROM %s WHERE ROUTINE_NAME = ? AND ROUTINE_TYPE = '%s'",
            GmsSystemTables.ROUTINES, FUNCTION);

    private final static String ALTER_FUNCTION =
        "UPDATE " + GmsSystemTables.ROUTINES + " SET %s WHERE ROUTINE_NAME = ? AND ROUTINE_TYPE = '" + FUNCTION + "'";

    private final static String ALTER_FUNCTION_WITH_COMMENT = "UPDATE " + GmsSystemTables.ROUTINES
        + " SET ROUTINE_COMMENT = ? , %s WHERE ROUTINE_NAME = ? AND ROUTINE_TYPE = '" + FUNCTION + "'";

    public int insertFunction(String functionName, String content, ExecutionContext executionContext) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            String currentTime = PLUtils.getCurrentTime();
            SQLCreateFunctionStatement
                statement = (SQLCreateFunctionStatement) FastsqlUtils.parseSql(content).get(0);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, functionName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, statement.getReturnDataType().getName());
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString,
                statement.toString(VisitorFeature.OutputPlOnlyDefinition));
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, statement.isDeterministic() ? "YES" : "NO");
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, statement.getSqlDataAccess().toString());
            MetaDbUtil.setParameter(7, params, ParameterMethod.setString,
                statement.getSqlSecurity().toString());
            MetaDbUtil.setParameter(8, params, ParameterMethod.setString, currentTime);
            MetaDbUtil.setParameter(9, params, ParameterMethod.setString, currentTime);
            // TODO sql_mode from execution content may not be accurate
            MetaDbUtil.setParameter(10, params, ParameterMethod.setString,
                Optional.ofNullable(executionContext.getSqlMode()).orElse(PlConstants.MOCK_SQL_MODE));
            MetaDbUtil.setParameter(11, params, ParameterMethod.setString,
                Optional.ofNullable(statement.getComment()).map(SQLTextLiteralExpr::getText).orElse(""));
            // TODO privilege info from execution content may not be accurate
            MetaDbUtil.setParameter(12, params, ParameterMethod.setString,
                Optional.ofNullable(statement.getDefiner()).map(
                    SQLName::getSimpleName).orElseGet(() -> {
                    return Optional.ofNullable(executionContext.getPrivilegeContext())
                        .map(t -> t.getUser() + "@" + t.getHost())
                        .orElse("");
                }));
            MetaDbUtil.setParameter(13, params, ParameterMethod.setString,
                getRoutineMeta(statement));
            int affectedRow = MetaDbUtil.insert(INSERT_FUNCTION, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    private String getRoutineMeta(SQLCreateFunctionStatement statement) {
        statement.setBlock(new SQLBlockStatement());
        return statement.toString(VisitorFeature.OutputPlOnlyDefinition);
    }

    public int dropFunction(String functionName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName);
            int affectedRow = MetaDbUtil.delete(DROP_FUNCTION, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public int alterFunction(String functionName, String content) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            SQLAlterFunctionStatement
                statement =
                (SQLAlterFunctionStatement) FastsqlUtils.parseSql(content, SQLParserFeature.IgnoreNameQuotes).get(0);
            String alterProcedure;
            if (statement.isExistsComment()) {
                alterProcedure = String.format(ALTER_FUNCTION_WITH_COMMENT, getModifyPart(statement));
            } else {
                alterProcedure = String.format(ALTER_FUNCTION, getModifyPart(statement));
            }
            int index = 1;
            if (statement.isExistsComment()) {
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString,
                    Optional.ofNullable(statement.getComment()).map(t -> t.getText()).orElse(""));
            }
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, functionName);
            int affectedRow = MetaDbUtil.delete(alterProcedure, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    private String getModifyPart(SQLAlterFunctionStatement statement) {
        StringBuilder sb = new StringBuilder();
        sb.append(" LAST_ALTERED = ").append("'").append(PLUtils.getCurrentTime()).append("'");
        if (statement.isExistsSqlSecurity()) {
            sb.append(" , ").append(" SECURITY_TYPE = ").append("'").append(statement.getSqlSecurity()).append("'");
        }
        if (statement.isExistsLanguageSql()) {
            sb.append(" , ").append(" ROUTINE_BODY = ").append("'").append(SQL).append("'");
        }
        return sb.toString();
    }

    public List<FunctionMetaRecord> loadFunctionMetas() {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            List<FunctionMetaRecord> records =
                MetaDbUtil.query(LOAD_FUNCTION_METAS, params, FunctionMetaRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public List<FunctionDefinitionRecord> getPushableFunctions() {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            List<FunctionDefinitionRecord> records =
                MetaDbUtil.query(GET_PUSHABLE_FUNCTIONS, params, FunctionDefinitionRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public List<ProcedureStatusRecord> getFunctionStatus(String like) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, like);
            List<ProcedureStatusRecord> records =
                MetaDbUtil.query(SHOW_FUNCTION_STATUS, params, ProcedureStatusRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public List<CreateProcedureRecord> getCreateFunction(String functionName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName);
            List<CreateProcedureRecord> records =
                MetaDbUtil.query(SHOW_CREATE_FUNCTION, params, CreateProcedureRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public List<FunctionDefinitionRecord> getFunctionDefinition(String functionName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName);
            List<FunctionDefinitionRecord> records =
                MetaDbUtil.query(FIND_FUNCTION_DEFINITION, params, FunctionDefinitionRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }
}
