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

package com.alibaba.polardbx.optimizer.core.function;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * 不带参数的函数 Created by lingce.ldm on 2016/11/4.
 */
public class FunctionWithoutArg extends SqlFunction {

    public FunctionWithoutArg(String functionName, SqlReturnTypeInference returnType) {
        super(functionName, SqlKind.OTHER_FUNCTION, returnType, null, OperandTypes.VARIADIC,
            SqlFunctionCategory.SYSTEM);
    }

    /**
     * {@code select database()}
     */
    public static FunctionWithoutArg DATABASE = new FunctionWithoutArg("DATABASE", ReturnTypes.VARCHAR_2000);
    public static FunctionWithoutArg LAST_INSERT_ID = new FunctionWithoutArg("LAST_INSERT_ID", ReturnTypes.BIGINT);
    public static FunctionWithoutArg UUID = new FunctionWithoutArg("UUID", ReturnTypes.VARCHAR_2000);
    public static FunctionWithoutArg UUID_SHORT = new FunctionWithoutArg("UUID_SHORT", ReturnTypes.BIGINT);
    public static FunctionWithoutArg PI = new FunctionWithoutArg("PI", ReturnTypes.DOUBLE);
    public static FunctionWithoutArg CONNECTION_ID = new FunctionWithoutArg("CONNECTION_ID",
        ReturnTypes.BIGINT_NULLABLE);
    public static FunctionWithoutArg TSO_TIMESTAMP = new FunctionWithoutArg("TSO_TIMESTAMP", ReturnTypes.BIGINT);
    public static FunctionWithoutArg LOCALTIME = new FunctionWithoutArg("LOCALTIME", ReturnTypes.TIMESTAMP);
    public static FunctionWithoutArg SCHEMA = new FunctionWithoutArg("SCHEMA", ReturnTypes.VARCHAR_2000);
    public static FunctionWithoutArg CHECK_FIREWORKS = new FunctionWithoutArg("CHECK_FIREWORKS", ReturnTypes.INTEGER);

    @Override
    public boolean canPushDown(boolean withScaleOut) {
        if (this == LAST_INSERT_ID || this == CONNECTION_ID || this == DATABASE || this == SCHEMA) {
            return false;
        }
        String name = getName();
        if ("CURRENT_USER".equals(name) || "VERSION".equals(name)) {
            return false;
        }
        return super.canPushDown(withScaleOut);
    }

    @Override
    public boolean isDynamicFunction() {
        return this == UUID || this == UUID_SHORT || this == LOCALTIME || super.isDynamicFunction();
    }

    /**
     * {@coce select now()}
     */
    public static class SqlNowFunction extends FunctionWithoutArg {

        public SqlNowFunction() {
            super("NOW", ReturnTypes.explicit(SqlTypeName.DATETIME));
        }

        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            SqlDialect dialect = writer.getDialect();
            // 对于Oracle将now()转换为sysdate
            if (dialect.getDatabaseProduct() == SqlDialect.DatabaseProduct.ORACLE) {
                writer.keyword("sysdate");
            } else {
                super.unparse(writer, call, leftPrec, rightPrec);
            }
        }

        @Override
        public boolean canPushDown(boolean withScaleOut) {
            if (withScaleOut) {
                return !isDynamicFunction();
            }
            return super.canPushDown(withScaleOut);
        }

        @Override
        public boolean isDynamicFunction() {
            return true;
        }
    }
}
