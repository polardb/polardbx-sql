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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group1;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

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
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.constant.TableConstant.PK_COLUMNS;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.GSI_PRIMARY_TABLE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * @author chenmo.cm
 */

public class CreateGsiPkTypeTest extends DDLBaseNewDBTestCase {
    private static final String CREATE_TEMPLATE = "CREATE TABLE {0} ({1} {2}) {3}";
    private static final String INSERT_TEMPLATE = "INSERT INTO {0}({1}) VALUES({2})";
    private static final ImmutableMap<String, List<String>> COLUMN_VALUES = GsiConstant.buildGsiFullTypeTestValues();
    private static final String GSI_NAME = "g_i_pk_type";

    private final String pk;
    private final String fullTypeTable;
    private Long rowCount;

    public CreateGsiPkTypeTest(String pk) {
        this.pk = pk;
        final String columnDef = FULL_TYPE_TABLE_COLUMNS.stream().map(
            c -> this.pk.equalsIgnoreCase(c) ? PK_COLUMN_DEF_MAP.get(c) : COLUMN_DEF_MAP.get(c)).collect(
            Collectors.joining());
        final String pkDef = PK_DEF_MAP.get(pk);
        final String partitionDef = GsiConstant.hashPartitioning(C_ID);
        fullTypeTable = MessageFormat.format(CREATE_TEMPLATE, GSI_PRIMARY_TABLE_NAME, columnDef, pkDef, partitionDef);
    }

    @Parameters(name = "{index}:pk={0}")
    public static List<String[]> prepareDate() {
        return PK_COLUMNS.stream().filter(
                c -> !C_ID.equalsIgnoreCase(c) && !C_TIMESTAMP.equalsIgnoreCase(c)).map(c -> new String[] {c})
            .collect(Collectors.toList());
    }

    @Before
    public void before() throws SQLException {
        dropTableWithGsi(GSI_PRIMARY_TABLE_NAME, ImmutableList.of(GSI_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, fullTypeTable);

        final List<String> pkValues = COLUMN_VALUES.get(pk);

        // Prepare data
        List<String> failedList = new ArrayList<>();
        for (int i = 0; i < pkValues.size(); i++) {
            final String pkValue = pkValues.get(i);

            final String insertSql = MessageFormat.format(INSERT_TEMPLATE, GSI_PRIMARY_TABLE_NAME,
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

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + GSI_PRIMARY_TABLE_NAME,
            tddlConnection);
        assertThat(resultSet.next(), is(true));
        this.rowCount = resultSet.getLong(1);
        assertThat(this.rowCount, greaterThan(0L));
    }

    @Test
    public void createGsi() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            "CREATE GLOBAL INDEX {0} ON {1}({2}) COVERING({3}) DBPARTITION BY HASH({2}) TBPARTITION BY HASH({2}) "
                + "TBPARTITIONS 3",
            GSI_NAME, GSI_PRIMARY_TABLE_NAME
            , C_ID, this.pk));

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + GSI_NAME,
            tddlConnection);
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getLong(1), equalTo(this.rowCount));
    }
}
