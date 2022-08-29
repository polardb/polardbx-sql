package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserDefinedJavaFunctionAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger("oss");
    private static final String FUNCTION_TABLE = wrap(GmsSystemTables.USER_DEFINED_JAVA_CODE);

    private static final String INSERT_FUNCTION = "insert ignore into " + FUNCTION_TABLE + "(id, func_name, class_name, code, code_language, input_types, result_type) values (null, ?, ?, ?, ?, ?, ?)";
    private static final String DELETE_FUNCTION = "delete from " + FUNCTION_TABLE + "where func_name='%s'";
    private static final String QUERY_ALL_FUNCTION = "select * from " + FUNCTION_TABLE;
    private static final String QUERY_FUNCTION_BY_NAME = "select * from " + FUNCTION_TABLE + "where func_name='%s'";

    public static void deleteFunctionByName(String funcName, Connection connection) {
        try {
            String deleteFunctionSql = String.format(DELETE_FUNCTION, funcName);
            MetaDbUtil.delete(deleteFunctionSql, connection);
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + FUNCTION_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                FUNCTION_TABLE,
                e.getMessage());
        }

    }

    public static void insertFunction(String funcName, String className, String code,
                                      String codeLanguage, Connection connection,
                                      String inputTypes, String resultType) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, funcName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, className);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, code);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, codeLanguage);
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, inputTypes);
        MetaDbUtil.setParameter(6, params, ParameterMethod.setString, resultType);

        try {
            MetaDbUtil.insert(INSERT_FUNCTION, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + FUNCTION_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                FUNCTION_TABLE,
                e.getMessage());
        }
    }

    public static List<UserDefinedJavaFunctionRecord> queryAllFunctions(Connection connection) {
        try {
            return MetaDbUtil.query(QUERY_ALL_FUNCTION, UserDefinedJavaFunctionRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table " + FUNCTION_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FUNCTION_TABLE,
                e.getMessage());
        }
    }

    public static List<UserDefinedJavaFunctionRecord> queryFunctionByName(String name, Connection connection) {
        try {
            return MetaDbUtil.query(String.format(QUERY_FUNCTION_BY_NAME, name), UserDefinedJavaFunctionRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table " + FUNCTION_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FUNCTION_TABLE,
                e.getMessage());
        }
    }
}
