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

package com.alibaba.polardbx.qatest.ddl.sharding.autoPartition;

import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.GsiConstant.COLUMN_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.GsiConstant.PK_COLUMN_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.GsiConstant.PK_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.constant.TableConstant.PK_COLUMNS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * @version 1.0
 */

public class AutoPartitionPkTypeTest extends AutoPartitionTestBase {
    public static final String AUTO_PARTITION_TABLE_NAME = "auto_partition_tb";

    private static final String CREATE_TEMPLATE = "CREATE PARTITION TABLE {0} ({1} {2})";
    private static final String INSERT_TEMPLATE = "INSERT INTO {0}({1}) VALUES({2})";
    private static final ImmutableMap<String, List<String>> COLUMN_VALUES = GsiConstant.buildGsiFullTypeTestValues();

    private final String pk;
    private final String fullTypeTable;
    private Long rowCount;

    public AutoPartitionPkTypeTest(String pk) {
        this.pk = pk;
        final String columnDef = FULL_TYPE_TABLE_COLUMNS.stream().map(
            c -> this.pk.equalsIgnoreCase(c) ? PK_COLUMN_DEF_MAP.get(c) : COLUMN_DEF_MAP.get(c)).collect(
            Collectors.joining());
        final String pkDef = PK_DEF_MAP.get(pk);
        fullTypeTable = MessageFormat.format(CREATE_TEMPLATE, AUTO_PARTITION_TABLE_NAME, columnDef, pkDef);
    }

    @Parameterized.Parameters(name = "{index}:pk={0}")
    public static List<String[]> prepareDate() {
        return PK_COLUMNS.stream()
            .filter(c -> !C_ID.equalsIgnoreCase(c))
            .filter(c -> !c.contains("_bit_"))
            .filter(c -> !c.contains("_decimal"))
            .filter(c -> !c.contains("_float"))
            .filter(c -> !c.contains("_double"))
            .filter(c -> !c.contains("_time"))
            .filter(c -> !c.contains("_year"))
            .filter(c -> !c.contains("_blob"))
            .filter(c -> !c.contains("_text"))
            .filter(c -> !c.contains("_enum"))
            .filter(c -> !c.contains("_set"))
            .filter(c -> !c.contains("_binary"))
            .filter(c -> !c.contains("_varbinary"))
            .map(c -> new String[] {c})
            .collect(Collectors.toList());
    }

    @Test
    public void createTableTest() throws SQLException {

        dropTableIfExists(AUTO_PARTITION_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, fullTypeTable);

        final List<String> pkValues = COLUMN_VALUES.get(pk);

        // Prepare data
        List<String> failedList = new ArrayList<>();
        for (int i = 0; i < pkValues.size(); i++) {
            final String pkValue = pkValues.get(i);

            final String insertSql = MessageFormat.format(INSERT_TEMPLATE, AUTO_PARTITION_TABLE_NAME,
                String.join(",", C_ID, this.pk),
                String.join(",", String.valueOf(i), pkValue));

            Statement ps = null;
            try {
                ps = tddlConnection.createStatement();
                ps.executeUpdate(insertSql);
            } catch (SQLSyntaxErrorException msee) {
                throw msee;
            } catch (SQLException e) {
                // ignore exception
                failedList.add(MessageFormat
                    .format("pk[{0}] value[{1}] error[{2}]", this.pk, String.valueOf(pkValue), e.getMessage()));
            } finally {
                JdbcUtil.close(ps);
            }
        }

        System.out.println("Failed inserts: ");
        failedList.forEach(System.out::println);

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + AUTO_PARTITION_TABLE_NAME,
            tddlConnection);
        assertThat(resultSet.next(), is(true));
        this.rowCount = resultSet.getLong(1);
        assertThat(this.rowCount, greaterThan(0L));
    }

}
