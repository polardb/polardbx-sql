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

package com.alibaba.polardbx.optimizer.core.datatype;

import org.apache.calcite.rel.type.RelDataType;
import org.junit.Assert;
import org.junit.Test;

public class DataTypeUtilTest {

    @Test
    public void testJdbcTypeToRelDataType() {
        final TypeCase[] jdbcTypes = {
            new TypeCase(-5, "BIGINT", 19, 0, 11, false, "BIGINT"),
            new TypeCase(-6, "TINYINT", 3, 0, 4, true, "TINYINT(3)"),
            new TypeCase(5, "SMALLINT", 5, 0, 6, true, "SMALLINT"),
            new TypeCase(4, "MEDIUMINT", 7, 0, 9, true, "MEDIUMINT"),
            new TypeCase(-5, "BIGINT", 19, 0, 20, true, "BIGINT"),
            new TypeCase(4, "INT", 10, 0, 11, true, "INTEGER"),
            new TypeCase(-7, "BIT", 1, 0, 1, true, "BIT(1)"),
            new TypeCase(-7, "TINYINT", 3, 0, 1, true, "TINYINT(1)"),
            new TypeCase(-7, "TINYINT", 3, 0, 1, true, "TINYINT(1)"),
            new TypeCase(5, "SMALLINT", 5, 0, 2, true, "SMALLINT"),
            new TypeCase(4, "MEDIUMINT", 7, 0, 3, true, "MEDIUMINT"),
            new TypeCase(4, "INT", 10, 0, 4, true, "INTEGER"),
            new TypeCase(-7, "BIT", 6, 0, 6, true, "BIG_BIT(6)"),
            new TypeCase(-6, "TINYINT UNSIGNED", 3, 0, 3, true, "TINYINT_UNSIGNED(3)"),
            new TypeCase(5, "SMALLINT UNSIGNED", 5, 0, 5, true, "SMALLINT_UNSIGNED"),
            new TypeCase(4, "MEDIUMINT UNSIGNED", 7, 0, 8, true, "MEDIUMINT_UNSIGNED"),
            new TypeCase(4, "INT UNSIGNED", 10, 0, 10, true, "INTEGER_UNSIGNED"),
            new TypeCase(-5, "BIGINT UNSIGNED", 20, 0, 20, true, "BIGINT_UNSIGNED"),
            new TypeCase(-7, "TINYINT UNSIGNED", 3, 0, 1, true, "TINYINT_UNSIGNED(1)"),
            new TypeCase(5, "SMALLINT UNSIGNED", 5, 0, 2, true, "SMALLINT_UNSIGNED"),
            new TypeCase(4, "MEDIUMINT UNSIGNED", 7, 0, 3, true, "MEDIUMINT_UNSIGNED"),
            new TypeCase(4, "INT UNSIGNED", 10, 0, 4, true, "INTEGER_UNSIGNED"),
            new TypeCase(-5, "BIGINT UNSIGNED", 20, 0, 5, true, "BIGINT_UNSIGNED"),
            new TypeCase(-6, "TINYINT UNSIGNED", 3, 0, 3, true, "TINYINT_UNSIGNED(3)"),
            new TypeCase(5, "SMALLINT UNSIGNED", 5, 0, 5, true, "SMALLINT_UNSIGNED"),
            new TypeCase(4, "MEDIUMINT UNSIGNED", 7, 0, 8, true, "MEDIUMINT_UNSIGNED"),
            new TypeCase(4, "INT UNSIGNED", 10, 0, 10, true, "INTEGER_UNSIGNED"),
            new TypeCase(-5, "BIGINT UNSIGNED", 20, 0, 20, true, "BIGINT_UNSIGNED"),
            new TypeCase(-6, "TINYINT UNSIGNED", 3, 0, 3, true, "TINYINT_UNSIGNED(3)"),
            new TypeCase(5, "SMALLINT UNSIGNED", 5, 0, 5, true, "SMALLINT_UNSIGNED"),
            new TypeCase(4, "MEDIUMINT UNSIGNED", 7, 0, 8, true, "MEDIUMINT_UNSIGNED"),
            new TypeCase(4, "INT UNSIGNED", 10, 0, 10, true, "INTEGER_UNSIGNED"),
            new TypeCase(-5, "BIGINT UNSIGNED", 20, 0, 20, true, "BIGINT_UNSIGNED"),
            new TypeCase(-7, "TINYINT UNSIGNED", 3, 0, 1, true, "TINYINT_UNSIGNED(1)"),
            new TypeCase(5, "SMALLINT UNSIGNED", 5, 0, 2, true, "SMALLINT_UNSIGNED"),
            new TypeCase(4, "MEDIUMINT UNSIGNED", 7, 0, 3, true, "MEDIUMINT_UNSIGNED"),
            new TypeCase(4, "INT UNSIGNED", 10, 0, 4, true, "INTEGER_UNSIGNED"),
            new TypeCase(-5, "BIGINT UNSIGNED", 20, 0, 5, true, "BIGINT_UNSIGNED"),
            new TypeCase(7, "FLOAT", 12, 0, 12, true, "FLOAT"),
            new TypeCase(8, "DOUBLE", 22, 0, 22, true, "DOUBLE"),
            new TypeCase(3, "DECIMAL", 10, 0, 11, true, "DECIMAL(10, 0)"),
            new TypeCase(8, "DOUBLE", 22, 0, 22, true, "DOUBLE"),
            new TypeCase(7, "FLOAT", 7, 3, 7, true, "FLOAT"),
            new TypeCase(8, "DOUBLE", 7, 3, 7, true, "DOUBLE"),
            new TypeCase(3, "DECIMAL", 7, 3, 9, true, "DECIMAL(7, 3)"),
            new TypeCase(8, "DOUBLE", 7, 3, 7, true, "DOUBLE"),
            new TypeCase(7, "FLOAT UNSIGNED", 12, 0, 12, true, "FLOAT"),
            new TypeCase(8, "DOUBLE UNSIGNED", 22, 0, 22, true, "DOUBLE"),
            new TypeCase(3, "DECIMAL UNSIGNED", 10, 0, 10, true, "DECIMAL(10, 0)"),
            new TypeCase(8, "DOUBLE UNSIGNED", 22, 0, 22, true, "DOUBLE"),
            new TypeCase(7, "FLOAT UNSIGNED", 7, 3, 7, true, "FLOAT"),
            new TypeCase(8, "DOUBLE UNSIGNED", 7, 3, 7, true, "DOUBLE"),
            new TypeCase(3, "DECIMAL UNSIGNED", 7, 3, 8, true, "DECIMAL(7, 3)"),
            new TypeCase(8, "DOUBLE UNSIGNED", 7, 3, 7, true, "DOUBLE"),
            new TypeCase(7, "FLOAT UNSIGNED", 12, 0, 12, true, "FLOAT"),
            new TypeCase(8, "DOUBLE UNSIGNED", 22, 0, 22, true, "DOUBLE"),
            new TypeCase(3, "DECIMAL UNSIGNED", 10, 0, 10, true, "DECIMAL(10, 0)"),
            new TypeCase(8, "DOUBLE UNSIGNED", 22, 0, 22, true, "DOUBLE"),
            new TypeCase(7, "FLOAT UNSIGNED", 7, 3, 7, true, "FLOAT"),
            new TypeCase(8, "DOUBLE UNSIGNED", 7, 3, 7, true, "DOUBLE"),
            new TypeCase(3, "DECIMAL UNSIGNED", 7, 3, 8, true, "DECIMAL(7, 3)"),
            new TypeCase(8, "DOUBLE UNSIGNED", 7, 3, 7, true, "DOUBLE"),
            new TypeCase(7, "FLOAT UNSIGNED", 12, 0, 12, true, "FLOAT"),
            new TypeCase(8, "DOUBLE UNSIGNED", 22, 0, 22, true, "DOUBLE"),
            new TypeCase(3, "DECIMAL UNSIGNED", 10, 0, 10, true, "DECIMAL(10, 0)"),
            new TypeCase(8, "DOUBLE UNSIGNED", 22, 0, 22, true, "DOUBLE"),
            new TypeCase(7, "FLOAT UNSIGNED", 7, 3, 7, true, "FLOAT"),
            new TypeCase(8, "DOUBLE UNSIGNED", 7, 3, 7, true, "DOUBLE"),
            new TypeCase(3, "DECIMAL UNSIGNED", 7, 3, 8, true, "DECIMAL(7, 3)"),
            new TypeCase(8, "DOUBLE UNSIGNED", 7, 3, 7, true, "DOUBLE"),
            new TypeCase(1, "CHAR", 10, 0, 0, true, "CHAR(10)"),
            new TypeCase(12, "VARCHAR", 1020, 0, 0, true, "VARCHAR(1020)"),
            new TypeCase(-2, "BINARY", 255, 0, 0, true, "BINARY(255)"),
            new TypeCase(-3, "VARBINARY", 255, 0, 0, true, "VARBINARY(255)"),
            new TypeCase(-3, "TINYBLOB", 0, 0, 0, true, "BLOB"),
            new TypeCase(-1, "VARCHAR", 1020, 0, 0, true, "VARCHAR(1020)"),
            new TypeCase(-1, "VARCHAR", 67108860, 0, 0, true, "VARCHAR(65536)"),
            new TypeCase(-1, "VARCHAR", 262140, 0, 0, true, "VARCHAR(65536)"),
            new TypeCase(-3, "TINYBLOB", 0, 0, 0, true, "BLOB"),
            new TypeCase(-4, "MEDIUMBLOB", 0, 0, 0, true, "BLOB"),
            new TypeCase(-4, "BLOB", 0, 0, 0, true, "BLOB"),
            new TypeCase(1, "enum('x-small','small','medium','large','x-large')", 0, 0, 28, true,
                "ENUM(large, medium, small, x-large, x-small)"),
            new TypeCase(1, "CHAR", 50, 0, 28, true, "CHAR(50)"),
            new TypeCase(91, "DATE", 0, 0, 10, true, "DATE"),
            new TypeCase(92, "TIME", 0, 0, 10, true, "TIME(0)"),
            new TypeCase(91, "YEAR", 0, 0, 4, true, "YEAR"),
            new TypeCase(93, "DATETIME", 0, 0, 19, true, "DATETIME(0)"),
            new TypeCase(93, "TIMESTAMP", 0, 0, 19, true, "TIMESTAMP(0)"),
            new TypeCase(91, "YEAR", 0, 0, 4, true, "YEAR"),
            new TypeCase(92, "TIME", 0, 0, 15, true, "TIME(0)"),
            new TypeCase(93, "DATETIME", 0, 0, 24, true, "DATETIME(0)"),
        };

        for (TypeCase t : jdbcTypes) {
            RelDataType dataType =
                DataTypeUtil.jdbcTypeToRelDataType(t.jdbcType, t.typeName, t.precision, t.scale, t.length, t.nullable);
            Assert.assertEquals(t.expectedTypeString, dataType.toString());
        }
    }

    static class TypeCase {
        final int jdbcType;
        final String typeName;
        final int precision;
        final int scale;
        final long length;
        final boolean nullable;

        final String expectedTypeString;

        public TypeCase(int jdbcType, String typeName, int precision, int scale, long length, boolean nullable,
                        String expectedTypeString) {
            this.jdbcType = jdbcType;
            this.typeName = typeName;
            this.precision = precision;
            this.scale = scale;
            this.length = length;
            this.nullable = nullable;
            this.expectedTypeString = expectedTypeString;
        }
    }
}
