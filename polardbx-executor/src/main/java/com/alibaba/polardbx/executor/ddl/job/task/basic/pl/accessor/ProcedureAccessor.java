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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.pl.procedure.CreateProcedureRecord;
import com.alibaba.polardbx.gms.metadb.pl.procedure.ProcedureMetaRecord;
import com.alibaba.polardbx.gms.metadb.pl.procedure.ProcedureDefinitionRecord;
import com.alibaba.polardbx.gms.metadb.pl.procedure.ProcedureStatusRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.*;

public class ProcedureAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(ProcedureAccessor.class);

    static final String DROP_PROCEDURE =
        String.format("DELETE FROM %s WHERE ROUTINE_NAME = ? AND ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = '%s'",
            GmsSystemTables.ROUTINES, PROCEDURE);

    static final String DROP_SCHEMA_PROCEDURE =
        String.format("DELETE FROM %s WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = '%s'",
            GmsSystemTables.ROUTINES, PROCEDURE);

    static final String INSERT_PROCEDURE = String.format("INSERT INTO %s " +
            " (SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, "
            + "DATA_TYPE, ROUTINE_BODY, ROUTINE_DEFINITION, PARAMETER_STYLE, IS_DETERMINISTIC"
            + ", SQL_DATA_ACCESS, SECURITY_TYPE, CREATED, LAST_ALTERED, SQL_MODE, ROUTINE_COMMENT"
            + ", DEFINER, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION, ROUTINE_META) "
            + "VALUES (?, '%s', ?, ?, '%s', '' , '%s', ?, '%s', ?, ?, "
            + " ?, ?, ?, ?, ?, ?, '%s', '%s', '%s', ?);", GmsSystemTables.ROUTINES, DEF_ROUTINE_CATALOG,
        PROCEDURE, SQL, SQL, MOCK_CHARACTER_SET_CLIENT, MOCK_COLLATION_CONNECTION, MOCK_DATABASE_COLLATION);

    static final String LOAD_PROCEDURE_METAS =
        String.format("SELECT ROUTINE_NAME, ROUTINE_SCHEMA FROM routines WHERE ROUTINE_TYPE = '%s'", PROCEDURE);

    private final static String FIND_PROCEDURE_DEFINITION =
        String.format(
            "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_DEFINITION FROM %s WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ? AND ROUTINE_TYPE = '%s'",
            GmsSystemTables.ROUTINES, PROCEDURE);

    private final static String SHOW_CREATE_PROCEDURE =
        String.format("SELECT ROUTINE_SCHEMA, ROUTINE_NAME, SQL_MODE, ROUTINE_DEFINITION, CHARACTER_SET_CLIENT, "
                + "COLLATION_CONNECTION, DATABASE_COLLATION FROM %s WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ? AND ROUTINE_TYPE = '%s'",
            GmsSystemTables.ROUTINES, PROCEDURE);

    private final static String SHOW_PROCEDURE_STATUS =
        String.format("SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, "
                + "DEFINER, LAST_ALTERED, CREATED, SECURITY_TYPE, ROUTINE_COMMENT, CHARACTER_SET_CLIENT, "
                + "COLLATION_CONNECTION, DATABASE_COLLATION FROM %s WHERE ROUTINE_NAME LIKE ? AND ROUTINE_TYPE = '%s'",
            GmsSystemTables.ROUTINES, PROCEDURE);

    private final static String ALTER_PROCEDURE = "UPDATE " + GmsSystemTables.ROUTINES
        + " SET %s WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ? AND ROUTINE_TYPE = '" + PROCEDURE + "'";

    private final static String ALTER_PROCEDURE_WITH_COMMENT = "UPDATE " + GmsSystemTables.ROUTINES
        + " SET ROUTINE_COMMENT = ? , %s WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ? AND ROUTINE_TYPE = '" + PROCEDURE
        + "'";

    public int insertProcedure(String schema,
                               String procedureName, String content,
                               ExecutionContext executionContext) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            String currentTime = PLUtils.getCurrentTime();
            SQLCreateProcedureStatement statement =
                (SQLCreateProcedureStatement) FastsqlUtils.parseSql(content).get(0);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, procedureName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, procedureName);
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
                Optional.ofNullable(executionContext.getSqlMode()).orElse(
                    PlConstants.MOCK_SQL_MODE));
            MetaDbUtil.setParameter(11, params, ParameterMethod.setString,
                Optional.ofNullable(statement.getComment()).map(
                    SQLTextLiteralExpr::getText).orElse(""));
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
            int affectedRow = MetaDbUtil.insert(INSERT_PROCEDURE, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    private String getRoutineMeta(SQLCreateProcedureStatement statement) {
        statement.setBlock(new SQLBlockStatement());
        return statement.toString(VisitorFeature.OutputPlOnlyDefinition);
    }

    public int dropProcedure(String schema, String procedureName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, procedureName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schema);
            int affectedRow = MetaDbUtil.delete(DROP_PROCEDURE, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public int alterProcedure(String schema,
                              String procedureName, String content) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            SQLAlterProcedureStatement statement =
                (SQLAlterProcedureStatement) FastsqlUtils.parseSql(content, SQLParserFeature.IgnoreNameQuotes).get(0);
            String alterProcedure;
            if (statement.isExistsComment()) {
                alterProcedure = String.format(ALTER_PROCEDURE_WITH_COMMENT, getModifyPart(statement));
            } else {
                alterProcedure = String.format(ALTER_PROCEDURE, getModifyPart(statement));
            }
            int index = 1;
            if (statement.isExistsComment()) {
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString,
                    Optional.ofNullable(statement.getComment()).map(t -> t.getText()).orElse(""));
            }
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(index, params, ParameterMethod.setString, procedureName);
            int affectedRow = MetaDbUtil.delete(alterProcedure, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    private String getModifyPart(SQLAlterProcedureStatement statement) {
        StringBuilder sb = new StringBuilder();
        sb.append(" LAST_ALTERED = ").append("'").append(PLUtils.getCurrentTime()).append("'");
        if (statement.isExistsSqlSecurity()) {
            sb.append(" , ").append(" SECURITY_TYPE = ").append("'").append(statement.getSqlSecurity()).append("'");
        }
        if (statement.isExistsLanguageSql()) {
            sb.append(" , ").append(" ROUTINE_BODY = ").append("'").append(SQL).append("'");
        }
        if (statement.isExistsSqlDataAccess()) {
            sb.append(" , ").append(" SQL_DATA_ACCESS = ").append("'").append(statement.getSqlDataAccess()).append("'");
        }
        return sb.toString();
    }

    public int dropRelatedProcedure(String schema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            int affectedRow = MetaDbUtil.delete(DROP_SCHEMA_PROCEDURE, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public List<ProcedureDefinitionRecord> getProcedureDefinition(String schema, String procedureName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, procedureName);
            List<ProcedureDefinitionRecord> records =
                MetaDbUtil.query(FIND_PROCEDURE_DEFINITION, params, ProcedureDefinitionRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public List<ProcedureMetaRecord> loadProcedureMetas() {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            List<ProcedureMetaRecord> records =
                MetaDbUtil.query(LOAD_PROCEDURE_METAS, params, ProcedureMetaRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public List<CreateProcedureRecord> getCreateProcedure(String schema, String procedureName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, procedureName);
            List<CreateProcedureRecord> records =
                MetaDbUtil.query(SHOW_CREATE_PROCEDURE, params, CreateProcedureRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }

    public List<ProcedureStatusRecord> getProcedureStatus(String like) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, like);
            List<ProcedureStatusRecord> records =
                MetaDbUtil.query(SHOW_PROCEDURE_STATUS, params, ProcedureStatusRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GmsSystemTables.ROUTINES + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }
}
