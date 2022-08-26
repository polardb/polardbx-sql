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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group3;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.Litmus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.StringContains.containsString;

/**
 * @author chenmo.cm
 */

public class GsiPartitionKeyDefaultValueTest extends DDLBaseNewDBTestCase {
    private static final Logger log = LoggerFactory.getLogger(
        GsiPartitionKeyDefaultValueTest.class);

    private static final String DEFAULT_PAD_DEF = "pad varchar(256) default null";
    private static final String DEFAULT_PK_DEF = "pk int not null primary key auto_increment";
    private static final String PRIMARY_NAME = "default_value_test_primary";
    private static final String GSI_NAME = "g_i_default";

    private final String primaryKeyDef;
    private final String primaryPartitionColumnDef;
    private final String gsiPartitionColumnDef;
    private final String primaryPartitionDef;
    private final String gsiDef;
    private final List<String> skList;
    private final int iValue;

    public GsiPartitionKeyDefaultValueTest(String primaryKeyDef, String primaryPartitionColumnDef,
                                           String gsiPartitionColumnDef, String primaryPartitionDef,
                                           String gsiDef, String skList, String iValue) {
        this.primaryKeyDef = TStringUtil.isBlank(primaryKeyDef) ? DEFAULT_PK_DEF : primaryKeyDef;
        this.primaryPartitionColumnDef = primaryPartitionColumnDef;
        this.gsiPartitionColumnDef = gsiPartitionColumnDef;
        this.primaryPartitionDef = primaryPartitionDef;
        this.gsiDef = gsiDef;
        this.skList = Arrays.asList(TStringUtil.split(skList, ","));
        this.iValue = Integer.parseInt(iValue);
    }

    private static final Set<String> skippedColumn = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private static final Set<String> dateColumn = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private static final Set<String> timestampColumn = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        skippedColumn.add("c_geometory");
        skippedColumn.add("c_point");
        skippedColumn.add("c_linestring");
        skippedColumn.add("c_polygon");
        skippedColumn.add("c_multipoint");
        skippedColumn.add("c_multilinestring");
        skippedColumn.add("c_multipolygon");
        skippedColumn.add("c_geometrycollection");

        dateColumn.add("c_date");
        dateColumn.add("c_datetime");
        dateColumn.add("c_datetime_1");
        dateColumn.add("c_datetime_3");
        dateColumn.add("c_datetime_6");
        timestampColumn.add("c_timestamp");
        timestampColumn.add("c_timestamp_1");
        timestampColumn.add("c_timestamp_3");
        timestampColumn.add("c_timestamp_6");
    }

    @Parameterized.Parameters(
        name = "{index}:pkDef={0}, primarySkDef={1}, gsiSkColumn={2}, primaryPartitionDef={3}, gsiDef={4}, skList={5}, iValue={6}")
    public static List<String[]> prepareDate() {
        final List<String[]> result = new ArrayList<>();
//        result.add(new String[] {
//            DEFAULT_PK_DEF, "c1 varchar(10) default null", "c2 varchar(10) default null", "dbpartition by hash(c1)",
//            "(c2) covering(pad) dbpartition by hash(c2)", "c1,c2"});
        GsiConstant.FULL_TYPE_TEST_VALUES.forEach((columnName, values) -> {
            if (skippedColumn.contains(columnName)) {
                return;
            }

            final String gsiColumnName = columnName + "_gsi";
            final String columnType = GsiConstant.COLUMN_TYPE_MAP.get(columnName);

            if (!dateColumn.contains(columnName) && !timestampColumn.contains(columnName)) {
                // DEFAULT NULL
                final String nullPrimarySkDef =
                    MessageFormat.format("`{0}` {1} NULL DEFAULT NULL", columnName, columnType);
                final String nullGsiSkDef =
                    MessageFormat.format("`{0}` {1} NULL DEFAULT NULL", gsiColumnName, columnType);
                final String nullPrimaryPartitionDef = getPartitionDef(columnName);
                final String nullGsiDef =
                    MessageFormat.format("({0}) COVERING(pad) {1}", gsiColumnName, getPartitionDef(gsiColumnName));

                result.add(new String[] {
                    DEFAULT_PK_DEF, nullPrimarySkDef, nullGsiSkDef, nullPrimaryPartitionDef, nullGsiDef,
                    String.join(",", columnName, gsiColumnName), "-1"});
            }

            // DEFAULT NOT NULL
            Ord.zip(values).forEach(o -> {
                final String value = o.getValue();
                final String primarySkDef = "null".equalsIgnoreCase(value) ?
                    MessageFormat.format("`{0}` {1} NULL DEFAULT {2}", columnName, columnType, value)
                    : MessageFormat.format("`{0}` {1} NOT NULL DEFAULT {2}", columnName, columnType, value);
                final String gsiSkDef = "null".equalsIgnoreCase(value) ?
                    MessageFormat.format("`{0}` {1} NULL DEFAULT {2}", gsiColumnName, columnType, value)
                    : MessageFormat.format("`{0}` {1} NOT NULL DEFAULT {2}", gsiColumnName, columnType, value);
                final String primaryPartitionDef = getPartitionDef(columnName);
                final String gsiDef =
                    MessageFormat.format("({0}) COVERING(pad) {1}", gsiColumnName, getPartitionDef(gsiColumnName));

                result.add(new String[] {
                    DEFAULT_PK_DEF, primarySkDef, gsiSkDef, primaryPartitionDef, gsiDef,
                    String.join(",", columnName, gsiColumnName), o.getKey().toString()});
            });
        });
        return result;
    }

    public static String getPartitionDef(String columnName) {
        final String oriColumnName = columnName;
        if (columnName.endsWith("_gsi")) {
            columnName = columnName.substring(0, columnName.length() - 4);
        }

        if (dateColumn.contains(columnName)) {
            return MessageFormat.format("DBPARTITION BY YYYYMM({0})", oriColumnName);
        }

        if (timestampColumn.contains(columnName)) {
            return MessageFormat.format("DBPARTITION BY YYYYMM({0})", oriColumnName);
        }

        return MessageFormat.format("DBPARTITION BY HASH({0})", oriColumnName);
    }

    private String buildCreateTableWithGsi() {
        final String template =
            "create table " + PRIMARY_NAME + "({0}, {1}, {2}, {3}, global index `" + GSI_NAME
                + "` {4}){5}";
        return MessageFormat
            .format(template, primaryKeyDef, primaryPartitionColumnDef, gsiPartitionColumnDef, DEFAULT_PAD_DEF, gsiDef,
                primaryPartitionDef);
    }

    private String buildCreateTableForMySQL() {
        final String template =
            "create table " + PRIMARY_NAME + "({0}, {1}, {2}, {3})";
        return MessageFormat
            .format(template, primaryKeyDef, primaryPartitionColumnDef, gsiPartitionColumnDef, DEFAULT_PAD_DEF, gsiDef,
                primaryPartitionDef);
    }

    public String getDefaultValue(String sk) {
        if (iValue < 0) {
            return "null";
        }

        if (sk.endsWith("_gsi")) {
            sk = sk.substring(0, sk.length() - 4);
        }

        return GsiConstant.FULL_TYPE_TEST_VALUES.get(sk).get(iValue);
    }

    public String getQueryValue(String sk) {
        final String oriSk = sk;
        if (sk.endsWith("_gsi")) {
            sk = sk.substring(0, sk.length() - 4);
        }

        if (sk.equalsIgnoreCase("c_decimal")) {
            if (iValue == 1) {
                return "100";
            } else if (iValue == 2 || iValue == 3) {
                return "-100";
            }
        } else if (sk.equalsIgnoreCase("c_decimal_pr") && iValue == 3) {
            return "-100.000";
        } else if (sk.equalsIgnoreCase("c_date") && iValue == 2) {
            return "0000-00-00";
        } else if (sk.equalsIgnoreCase("c_datetime_1")) {
            switch (iValue) {
            case 2:
                return "'0000-00-00 01:01:01.1'";
            case 3:
                return "'1969-09-00 23:59:59.1'";
            case 4:
                return "'2018-00-00 00:00:00'";
            case 5:
                return "'2017-12-12 23:59:59'";
            }
        } else if (sk.equalsIgnoreCase("c_datetime_3")) {
            switch (iValue) {
            case 3:
                return "'1969-09-00 23:59:59.001'";
            case 4:
                return "'2018-00-00 00:00:00'";
            case 5:
                return "'2017-12-12 23:59:59'";
            }
        } else if (sk.equalsIgnoreCase("c_datetime_6")) {
            switch (iValue) {
            case 3:
                return "'1969-09-00 23:59:59.000001'";
            case 4:
                return "'2018-00-00 00:00:00'";
            case 5:
                return "'2017-12-12 23:59:59'";
            }
        } else if (sk.equalsIgnoreCase("c_timestamp_1")) {
            if (iValue == 5) {
                return "'2017-12-12 23:59:59'";
            }
        } else if (sk.equalsIgnoreCase("c_timestamp_3")) {
            if (iValue == 5) {
                return "'2017-12-12 23:59:59'";
            }
        } else if (sk.equalsIgnoreCase("c_set")) {
            switch (iValue) {
            case 1:
                return "'a,b'";
            case 2:
                return "'a,b,c'";
            }
        }

        return getDefaultValue(oriSk);
    }

    @Before
    public void before() {

        // Drop table
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_NAME);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_NAME);
        dropTableWithGsi(PRIMARY_NAME, ImmutableList.of(GSI_NAME));
    }

    @Test
    public void testEmpty() throws SQLException {
        // for avoid throwing ex of "java.lang.Exception:No runnable methods"
    }

    //FIXME mocheng,
    // failed because of error time zone of timestamp filed of default value during insert
    @Ignore
    public void test() throws SQLException {
        if (isMySQL80() && skList.stream().anyMatch(s -> TStringUtil.containsIgnoreCase(s, "c_varbinary"))) {
            // MySQL 8.0 use a hex string as default value of varbinary type column as value of "Default" column of DESC result
            return;
        }

        final String createTableWithGsi = buildCreateTableWithGsi();
        final String createTableForMySQL = buildCreateTableForMySQL();

        try (Statement stmt = mysqlConnection.createStatement()) {
            stmt.execute(createTableForMySQL);
        } catch (SQLSyntaxErrorException e) {
            log.error(e.getMessage(), e);
            Assert.assertThat(e.getMessage(),
                anyOf(containsString("Invalid default value for"), containsString("can't have a default value")));
            return;
        }

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(HINT_CREATE_GSI + createTableWithGsi);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            Assert.assertThat(e.getMessage(), anyOf(containsString("Rule generator dataType is not supported!"),
                containsString("Invalid type for a sharding key.")));
            return;
        }

        testSingleInsertOrReplace(false);
        // Compare data in mysql and drds
        checkData();
        // Compare data in primary and gsi
        checkGsi(tddlConnection, GSI_NAME);

//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + PRIMARY_NAME, null);
//
//        testSingleInsertOrReplace(true);
//        // Compare data in mysql and drds
//        checkData();
//        // Compare data in primary and gsi
//        checkGsi(tddlConnection, GSI_NAME);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + PRIMARY_NAME, null);

        testMultiValueInsertOrReplace(false);
        // Compare data in mysql and drds
        checkData();
        // Compare data in primary and gsi
        checkGsi(tddlConnection, GSI_NAME);

//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + PRIMARY_NAME, null);
//
//        testMultiValueInsertOrReplace(true);
//        // Compare data in mysql and drds
//        checkData();
//        // Compare data in primary and gsi
//        checkGsi(tddlConnection, GSI_NAME);

        testInsertOrReplaceSelect(false);
        // Compare data in mysql and drds
        checkData();
        // Compare data in primary and gsi
        checkGsi(tddlConnection, GSI_NAME);

//        testInsertOrReplaceSelect(true);
//        // Compare data in mysql and drds
//        checkData();
//        // Compare data in primary and gsi
//        checkGsi(tddlConnection, GSI_NAME);

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    public void testInsertOrReplaceSelect(boolean replace) {
        // Insert with explicit value foreach shard key
        String sql =
            MessageFormat
                .format((replace ? "REPLACE" : "INSERT") + " INTO {0}({1}, pad) select {2}, pad from {3}", PRIMARY_NAME,
                    String.join(",", skList),
                    skList.stream().map(this::getDefaultValue).collect(Collectors.joining(",")),
                    PRIMARY_NAME);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Insert with default value foreach shard key
        sql = MessageFormat
            .format((replace ? "REPLACE" : "INSERT") + " INTO {0}(pad) select \"{1}\" from {2}", PRIMARY_NAME,
                UUID.randomUUID().toString(), PRIMARY_NAME);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    public void testSingleInsertOrReplace(boolean replace) {
        // Insert with explicit value foreach shard key
        String sql =
            MessageFormat
                .format((replace ? "REPLACE" : "INSERT") + " INTO {0}({1}, pad) values({2}, {3})", PRIMARY_NAME,
                    String.join(",", skList),
                    skList.stream().map(this::getDefaultValue).collect(Collectors.joining(",")),
                    "'" + UUID.randomUUID().toString() + "'");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Insert with default value foreach shard key
        sql = MessageFormat.format((replace ? "REPLACE" : "INSERT") + " INTO {0}(pad) values(\"{1}\")", PRIMARY_NAME,
            UUID.randomUUID().toString());
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Insert with keyword DEFAULT foreach shard key
        sql = MessageFormat
            .format((replace ? "REPLACE" : "INSERT") + " INTO {0}({1}, pad) values({2}, \"{3}\")", PRIMARY_NAME,
                String.join(",", skList),
                skList.stream().map(sk -> "DEFAULT").collect(Collectors.joining(",")),
                UUID.randomUUID().toString());
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    public void testMultiValueInsertOrReplace(boolean replace) {
        // Insert with explicit value foreach shard key
        String sql =
            MessageFormat
                .format((replace ? "REPLACE" : "INSERT") + " INTO {0}({1}, pad) values({2}, {3}),({4},{5})",
                    PRIMARY_NAME,
                    String.join(",", skList),
                    skList.stream().map(this::getDefaultValue).collect(Collectors.joining(",")),
                    "'" + UUID.randomUUID().toString() + "'",
                    skList.stream().map(this::getDefaultValue).collect(Collectors.joining(",")),
                    "'" + UUID.randomUUID().toString() + "'"
                );

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Insert with default value foreach shard key
        sql = MessageFormat
            .format((replace ? "REPLACE" : "INSERT") + " INTO {0}(pad) values(\"{1}\"),(\"{2}\")", PRIMARY_NAME,
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Insert with keyword DEFAULT foreach shard key
        sql = MessageFormat
            .format((replace ? "REPLACE" : "INSERT") + " INTO {0}({1}, pad) values({2}, \"{3}\"), ({4}, \"{5}\")",
                PRIMARY_NAME,
                String.join(",", skList),
                skList.stream().map(sk -> "DEFAULT").collect(Collectors.joining(",")),
                UUID.randomUUID().toString(),
                skList.stream().map(sk -> "DEFAULT").collect(Collectors.joining(",")),
                UUID.randomUUID().toString()
            );
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    public void checkData() {
        // Compare data in mysql and primary
        String sql = MessageFormat.format("SELECT {0} , pad FROM {1} WHERE {2}", String.join(",", skList), PRIMARY_NAME,
            skList.stream().map(sk -> {
                final String defaultValue = getQueryValue(sk);
                if ("null".equalsIgnoreCase(defaultValue)) {
                    return sk + " IS NULL";
                } else {
                    return sk + " = " + defaultValue;
                }
            }).collect(Collectors.joining(" AND ")));
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        // Compare data in mysql and gsi
        final String gsiSql =
            MessageFormat.format("SELECT {0} , pad FROM {1} WHERE {2}", String.join(",", skList), GSI_NAME,
                skList.stream().map(sk -> {
                    final String defaultValue = getQueryValue(sk);
                    if ("null".equalsIgnoreCase(defaultValue)) {
                        return sk + " IS NULL";
                    } else {
                        return sk + " = " + defaultValue;
                    }
                }).collect(Collectors.joining(" AND ")));
        selectContentSameAssert(sql, gsiSql, null, mysqlConnection, tddlConnection, true);
    }
}
