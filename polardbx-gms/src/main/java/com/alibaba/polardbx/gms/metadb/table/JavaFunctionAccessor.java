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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaFunctionAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(JavaFunctionAccessor.class);
    private static final String FUNCTION_TABLE = wrap(GmsSystemTables.JAVA_FUNCTIONS);

    private static final String INSERT_FUNCTION = "insert into " + FUNCTION_TABLE
        + " (function_name, class_name, code, input_types, return_type, is_no_state) values (?, ?, ?, ?, ?, ?)";
    private static final String DROP_FUNCTION = "delete from " + FUNCTION_TABLE + " where function_name = ?";
    private static final String QUERY_ALL_FUNCTION_NAME =
        "select function_name, input_types, return_type, is_no_state from " + FUNCTION_TABLE;
    private static final String QUERY_FUNCTION_BY_NAME = "select * from " + FUNCTION_TABLE + " where function_name = ?";

    public int dropFunction(String funcName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, funcName);
            int affectedRow = MetaDbUtil.delete(DROP_FUNCTION, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + FUNCTION_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                FUNCTION_TABLE,
                e.getMessage());
        }
    }

    public int insertFunction(String funcName, String className, String code,
                              String inputTypes, String resultType, Boolean noState) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, funcName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, className);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, code);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, inputTypes);
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, resultType);
        MetaDbUtil.setParameter(6, params, ParameterMethod.setBoolean, noState);

        try {
            int affectedRow = MetaDbUtil.insert(INSERT_FUNCTION, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + FUNCTION_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                FUNCTION_TABLE,
                e.getMessage());
        }
    }

    public List<JavaFunctionMetaRecord> queryAllFunctionMetas() {
        try {
            return MetaDbUtil.query(QUERY_ALL_FUNCTION_NAME, JavaFunctionMetaRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table " + FUNCTION_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FUNCTION_TABLE,
                e.getMessage());
        }
    }

    public List<JavaFunctionRecord> queryFunctionByName(String name) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name);
            return MetaDbUtil.query(QUERY_FUNCTION_BY_NAME, params, JavaFunctionRecord.class,
                connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table " + FUNCTION_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FUNCTION_TABLE,
                e.getMessage());
        }
    }
}
