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
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLParameter;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.*;

/**
 * @author yuehan.wcf
 */
public class PlParameterAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(PlParameterAccessor.class);

    static String INSERT_PROCEDURE_PARAMS =
        String.format("INSERT INTO %s (SPECIFIC_CATALOG, SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION, "
                + "PARAMETER_MODE, PARAMETER_NAME, DATA_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, DTD_IDENTIFIER, ROUTINE_TYPE) "
                + "VALUES ('%s', ?, ?, ?, ?, ?, ?, '%s', '%s', ?, '%s')", GmsSystemTables.PARAMETERS, DEF_ROUTINE_CATALOG,
            MOCK_CHARACTER_SET_CLIENT, MOCK_COLLATION_CONNECTION, PROCEDURE);

    static String DROP_PROCEDURE_PARAMS =
        String.format("DELETE FROM %s WHERE SPECIFIC_SCHEMA = ? AND SPECIFIC_NAME = ?", GmsSystemTables.PARAMETERS);

    static String DROP_RELATED_PROCEDURE_PARAMS =
        String.format("DELETE FROM %s WHERE SPECIFIC_SCHEMA = ?", GmsSystemTables.PARAMETERS);

    static String INSERT_FUNCTION_PARAMS =
        String.format("INSERT INTO %s (SPECIFIC_CATALOG, SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION, "
                + "PARAMETER_MODE, PARAMETER_NAME, DATA_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, DTD_IDENTIFIER, ROUTINE_TYPE) "
                + "VALUES ('%s', '%s', ?, ?, ?, ?, ?, '%s', '%s', ?, '%s')", GmsSystemTables.PARAMETERS,
            DEF_ROUTINE_CATALOG,
            MYSQL, MOCK_CHARACTER_SET_CLIENT, MOCK_COLLATION_CONNECTION, FUNCTION);

    static String DROP_FUNCTION_PARAMS =
        String.format("DELETE FROM %s WHERE SPECIFIC_NAME = ?", GmsSystemTables.PARAMETERS);

    public int insertProcedureParams(String schema, String procedureName, String procedureContent) {
        SQLCreateProcedureStatement statement =
            (SQLCreateProcedureStatement) FastsqlUtils.parseSql(procedureContent, SQLParserFeature.IgnoreNameQuotes)
                .get(0);
        int ordinal = 1, affectedRow = 0;
        for (SQLParameter parameter : statement.getParameters()) {
            try {
                Map<Integer, ParameterContext> params = new HashMap<>();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
                MetaDbUtil.setParameter(2, params, ParameterMethod.setString, procedureName);
                MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, ordinal++);
                MetaDbUtil.setParameter(4, params, ParameterMethod.setString, replaceDefaultType(parameter));
                MetaDbUtil.setParameter(5, params, ParameterMethod.setString, parameter.getName().getSimpleName());
                MetaDbUtil.setParameter(6, params, ParameterMethod.setString, parameter.getDataType().getName());
                MetaDbUtil.setParameter(7, params, ParameterMethod.setString, parameter.getDataType().getName());
                affectedRow += MetaDbUtil.insert(INSERT_PROCEDURE_PARAMS, params, connection);
            } catch (Exception e) {
                logger.error("Failed to insert the system table '" + GmsSystemTables.PARAMETERS + "'", e);
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                    GmsSystemTables.PARAMETERS,
                    e.getMessage());
            }
        }
        return affectedRow;
    }

    public int insertFunctionParams(String functionName, String functionContent) {
        SQLCreateFunctionStatement statement =
            (SQLCreateFunctionStatement) FastsqlUtils.parseSql(functionContent, SQLParserFeature.IgnoreNameQuotes)
                .get(0);
        int ordinal = 1, affectedRow = 0;
        for (SQLParameter parameter : statement.getParameters()) {
            try {
                Map<Integer, ParameterContext> params = new HashMap<>();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName);
                MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, ordinal++);
                MetaDbUtil.setParameter(3, params, ParameterMethod.setString, replaceDefaultType(parameter));
                MetaDbUtil.setParameter(4, params, ParameterMethod.setString, parameter.getName().getSimpleName());
                MetaDbUtil.setParameter(5, params, ParameterMethod.setString, parameter.getDataType().getName());
                MetaDbUtil.setParameter(6, params, ParameterMethod.setString, parameter.getDataType().getName());
                affectedRow += MetaDbUtil.insert(INSERT_FUNCTION_PARAMS, params, connection);
            } catch (Exception e) {
                logger.error("Failed to insert the system table '" + GmsSystemTables.PARAMETERS + "'", e);
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                    GmsSystemTables.PARAMETERS,
                    e.getMessage());
            }
        }
        affectedRow += insertReturnType(functionName, statement.getReturnDataType());
        return affectedRow;
    }

    private int insertReturnType(String functionName, SQLDataType returnType) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, 0);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, null);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, null);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, returnType.getName());
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, returnType.getName());
            return MetaDbUtil.insert(INSERT_FUNCTION_PARAMS, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + GmsSystemTables.PARAMETERS + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.PARAMETERS,
                e.getMessage());
        }
    }

    private String replaceDefaultType(SQLParameter parameter) {
        return parameter.getParamType() == SQLParameter.ParameterType.DEFAULT ? "IN" :
            parameter.getParamType().toString();
    }

    public int dropProcedureParams(String schema, String procedureName) {
        int affectedRow = 0;
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, procedureName);
            affectedRow += MetaDbUtil.delete(DROP_PROCEDURE_PARAMS, params, connection);
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.PARAMETERS + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.PARAMETERS,
                e.getMessage());
        }
        return affectedRow;
    }

    public int dropFunctionParams(String functionName) {
        int affectedRow = 0;
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, functionName);
            affectedRow += MetaDbUtil.delete(DROP_FUNCTION_PARAMS, params, connection);
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.PARAMETERS + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.PARAMETERS,
                e.getMessage());
        }
        return affectedRow;
    }

    public int dropRelatedParams(String schema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            int affectedRow = MetaDbUtil.delete(DROP_RELATED_PROCEDURE_PARAMS, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.PARAMETERS + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GmsSystemTables.ROUTINES,
                e.getMessage());
        }
    }
}
