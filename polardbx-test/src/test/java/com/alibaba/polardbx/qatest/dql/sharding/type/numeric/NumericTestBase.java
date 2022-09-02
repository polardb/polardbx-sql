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

package com.alibaba.polardbx.qatest.dql.sharding.type.numeric;

import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;

public abstract class NumericTestBase extends CrudBasedLockTestCase {
    protected static final int BATCH_SIZE = 1 << 10;

    protected static final String DECIMAL_TEST_LOW = "decimal_test_low";
    protected static final String DECIMAL_TEST_HIGH = "decimal_test_high";
    protected static final String TINYINT_TEST = "tinyint_test";
    protected static final String UTINYINT_TEST = "utinyint_test";
    protected static final String SMALLINT_TEST = "smallint_test";
    protected static final String USMALLINT_TEST = "usmallint_test";
    protected static final String MEDIUMINT_TEST = "mediumint_test";
    protected static final String UMEDIUMINT_TEST = "umediumint_test";
    protected static final String INT_TEST = "int_test";
    protected static final String UINT_TEST = "uint_test";
    protected static final String BIGINT_TEST = "bigint_test";
    protected static final String UBIGINT_TEST = "ubigint_test";
    protected static final String LONG_NUMBER_TEST = "long_number_test";
    protected static final String VARCHAR_TEST = "varchar_test";
    protected static final String CHAR_TEST = "char_test";

    protected static final String INSERT_SQL_FORMAT = "insert into %s (%s) values (?)";
    protected static final String INSERT_SQL_FORMAT_2 = "insert into %s (%s, %s) values (?, ?)";
    protected static final String SQL_CREATE_TEST_TABLE = "create table if not exists %s (\n"
        + "  pk bigint auto_increment,\n"
        + "  decimal_test_high decimal(65, 30),\n"
        + "  decimal_test_low decimal(15, 5),\n"
        + "  tinyint_test tinyint,\n"
        + "  utinyint_test tinyint unsigned,\n"
        + "  smallint_test smallint,\n"
        + "  usmallint_test smallint unsigned,\n"
        + "  mediumint_test mediumint,\n"
        + "  umediumint_test mediumint unsigned,\n"
        + "  int_test int,\n"
        + "  uint_test int unsigned,\n"
        + "  bigint_test bigint,\n"
        + "  ubigint_test bigint unsigned,\n"
        + "  varchar_test varchar(100),\n"
        + "  char_test char(100),\n"
        + "  long_number_test varchar(100),\n"
        + "  float_test float,\n"
        + "  double_test double,\n"
        + "  primary key (pk)) %s";

    protected static final String SUFFIX = " dbpartition by hash(pk) tbpartition by hash(pk);";
    protected static final String SUFFIX_IN_NEW_PART = "partition by key(pk) partitions 8";

    public static final String DOUBLE_TEST = "double_test";
    public static final String FLOAT_TEST = "float_test";

    protected final String TEST_TABLE;

    public NumericTestBase(String tableName) {
        this.TEST_TABLE = tableName;
    }

    @Before
    public void prepareMySQLTable() {
        String mysqlSql = String.format(SQL_CREATE_TEST_TABLE, TEST_TABLE, "");
        JdbcUtil.executeSuccess(mysqlConnection, mysqlSql);
        JdbcUtil.executeSuccess(mysqlConnection, String.format("delete from %s where 1=1", TEST_TABLE));
    }

    @Before
    public void preparePolarDBXTable() {
        String tddlSql = String.format(
            SQL_CREATE_TEST_TABLE,
            TEST_TABLE,
            usingNewPartDb() ? SUFFIX_IN_NEW_PART : SUFFIX);
        JdbcUtil.executeSuccess(tddlConnection, tddlSql);
        JdbcUtil.executeSuccess(tddlConnection, String.format("delete from %s where 1=1", TEST_TABLE));
    }

    @After
    public void afterTable() {
        JdbcUtil.dropTable(tddlConnection, TEST_TABLE);
        JdbcUtil.dropTable(mysqlConnection, TEST_TABLE);
    }

    protected void insertData(List<Object> paramList, String col) {
        final String insertSql = String.format(INSERT_SQL_FORMAT, TEST_TABLE, col);
        List params = paramList.stream()
            .map(Collections::singletonList)
            .collect(Collectors.toList());

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);
    }

    protected void insertData(List<Object[]> paramList, String col1, String col2) {
        String insertSql;
        List params;
        if (col1.equals(col2)) {
            insertSql = String.format(INSERT_SQL_FORMAT, TEST_TABLE, col1);
            params = paramList.stream()
                .map(objects -> objects[0])
                .map(Collections::singletonList)
                .collect(Collectors.toList());
        } else {
            insertSql = String.format(INSERT_SQL_FORMAT_2, TEST_TABLE, col1, col2);
            params = paramList.stream()
                .map(Arrays::asList)
                .collect(Collectors.toList());
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);
    }

    protected Supplier<Object> getGenerator(final String col) {
        return getGenerator(col, false);
    }

    protected Supplier<Object> getGenerator(final String col, boolean hasNull) {
        switch (col) {
        case DECIMAL_TEST_HIGH:
            return () -> hasNull && CastTestUtils.useNull() ? null :
                CastTestUtils.randomValidDecimal(DecimalTypeBase.MAX_DECIMAL_PRECISION,
                    DecimalTypeBase.MAX_DECIMAL_SCALE);
        case DECIMAL_TEST_LOW:
            return () -> hasNull && CastTestUtils.useNull() ? null :
                CastTestUtils.randomValidDecimal(15, 5);
        case TINYINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomTinyint();
        case UTINYINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomUTinyint();
        case SMALLINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomSmallint();
        case USMALLINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomUSmallint();
        case MEDIUMINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomMediumInt();
        case UMEDIUMINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomUMediumInt();
        case INT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomInt();
        case UINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomUInt();
        case BIGINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomBigint();
        case UBIGINT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomUBigint();
        case LONG_NUMBER_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomLongStr();
        case FLOAT_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomFloat();
        case DOUBLE_TEST:
            return () -> hasNull && CastTestUtils.useNull() ? null : CastTestUtils.randomDouble();
        case VARCHAR_TEST:
        case CHAR_TEST:
        default: {
            // short integer / long integer / all decimal
            return () -> hasNull && CastTestUtils.useNull() ? null
                : (CastTestUtils.useNatural()
                ? CastTestUtils.randomValidDecimal(20, 0)
                : CastTestUtils.randomStr());
        }
        }
    }
}
