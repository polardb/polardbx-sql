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

package com.alibaba.polardbx.optimizer.core.dialect;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Created by lingce.ldm on 2016/12/5.
 */
public class MySqlDialect extends Dialect {
    private static final int DEFAULT_NUMBER_PRECISION = 1;

    @Override
    public SqlTypeName getSqlTypeName(String typeName) {
        String type = typeName;
        switch (typeName) {
        case "INT":
        case "MEDIUMINT":
        case "YEAR":
            type = "INTEGER";
            break;
        case "DATETIME":
            type = "TIMESTAMP";
            break;
        case "NUMERIC":
            type = "DECIMAL";
            break;
        case "REAL":
        case "DOUBLE PRECISION":
            type = "DOUBLE";
            break;
        case "TEXT":
        case "TINYTEXT":
        case "LONGTEXT":
        case "MEDIUMTEXT":
        case "ENUM":
        case "BIT":
        case "JSON":
            type = "VARCHAR";
            break;
        case "TINYBLOB":
        case "BLOB":
        case "MEDIUMBLOB":
        case "LONGBLOB":
            type = "BINARY";
            break;
        default:
            // Do nothing
        }
        return SqlTypeName.valueOf(type);
    }

    @Override
    public SqlDialect getCalciteSqlDialect() {
        return MysqlSqlDialect.DEFAULT;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "`";
    }

    @Override
    public String getDriverName() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    public String getValidationSql() {
        return "select 1";
    }

    /**
     * 如果精度为0，将其设置为1，
     * 否则从数据库中获取的BigDecimal字段精度为1，会导致错误
     */
    @Override
    public long getDecimalPrecision(long precision) {
        if (precision == 0) {
            return DEFAULT_NUMBER_PRECISION;
        }
        return precision;
    }
}
