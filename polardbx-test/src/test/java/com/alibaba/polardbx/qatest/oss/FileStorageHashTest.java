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

package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeSeparatedTestUtil;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

public class FileStorageHashTest extends BaseTestCase {
    private static String testDataBase = "fileStorageHashTest";

    private static Engine engine = PropertiesUtil.engine();

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    @BeforeClass
    static public void initTestDatabase() {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase));
            statement.execute(String.format("create database %s mode = 'auto'", testDataBase));
            statement.execute(String.format("use %s", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    static public void DropDatabase() {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOrcHashInt() {
        try (Connection connection = getConnection()) {
            String innoTable = "hash_test0";
            String ossTable = "hash_test0_oss";
            Statement statement = connection.createStatement();

            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id int NOT NULL,\n" +
                "    PRIMARY KEY (id)\n" +
                ")\n" +
                "PARTITION BY HASH(id) PARTITIONS 8\n", innoTable));
            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innoTable).append("('id') values");
            for (int i = 0; i < 9999; i++) {
                insert.append("(").append(i * 10).append("),");
            }
            insert.append("(100000);");
            statement.executeUpdate(insert.toString());

            createOssTableLoadingFromInnodbTable(connection, ossTable, innoTable);

            List<String> sqls = new ArrayList<>();
            sqls.add("select check_sum(*) from %s");
            sqls.add("select check_sum(id) from %s where id < 100");
            compareSqls(sqls, connection, ossTable, innoTable);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOrcHashDecimal() {
        try (Connection connection = getConnection()) {
            String innoTable = "hash_test1";
            String ossTable = "hash_test1_oss";
            Statement statement = connection.createStatement();

            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL,\n" +
                "    deci decimal(10,2) NOT NULL,\n" +
                "    PRIMARY KEY (id)\n" +
                ")\n" +
                "PARTITION BY HASH(id) PARTITIONS 8\n", innoTable));
            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innoTable).append("('id','deci') values");
            for (int i = 0; i < 9998; i++) {
                insert.append("(").append(i).append(", ").append(i * 0.1).append("),");
            }
            insert.append("(9999,999.91);");
            statement.executeUpdate(insert.toString());
            createOssTableLoadingFromInnodbTable(connection, ossTable, innoTable);

            List<String> sqls = new ArrayList<>();
            sqls.add("select check_sum(*) from %s");
            sqls.add("select check_sum(id, deci) from %s");
            sqls.add("select check_sum(deci) from %s where id < 100");
            compareSqls(sqls, connection, ossTable, innoTable);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOrcHashDate() {
        try (Connection connection = getConnection()) {
            String innoTable = "hash_test2";
            String ossTable = "hash_test2_oss";
            Statement statement = connection.createStatement();

            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL,\n" +
                "    gmt_modified DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                "    PRIMARY KEY (id, gmt_modified)\n" +
                ")\n" +
                "PARTITION BY HASH(id) PARTITIONS 8\n", innoTable));
            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innoTable).append("('id') values");
            for (int i = 0; i < 9998; i++) {
                insert.append("(").append(i).append("),");
            }
            insert.append("(10000);");
            statement.executeUpdate(insert.toString());

            createOssTableLoadingFromInnodbTable(connection, ossTable, innoTable);

            List<String> sqls = new ArrayList<>();
            sqls.add("select check_sum(*) from %s");
            sqls.add("select check_sum(id) from %s");
            sqls.add("select check_sum(gmt_modified) from %s where id < 100");
            compareSqls(sqls, connection, ossTable, innoTable);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOrcHashIntSingle() {
        try (Connection connection = getConnection()) {
            String innoTable = "hash_test4";
            String ossTable = "hash_test4_oss";
            Statement statement = connection.createStatement();

            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id int NOT NULL,\n" +
                "    PRIMARY KEY (id)\n" +
                ")\n" +
                "single\n", innoTable));
            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innoTable).append("('id') values");
            for (int i = 0; i < 999; i++) {
                insert.append("(").append(i * 10).append("),");
            }
            insert.append("(100000);");
            statement.executeUpdate(insert.toString());

            createOssTableLoadingFromInnodbTable(connection, ossTable, innoTable);

            List<String> sqls = new ArrayList<>();
            sqls.add("select check_sum(*) from %s");
            sqls.add("select check_sum(id) from %s where id < 100");
            compareSqls(sqls, connection, ossTable, innoTable);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOrcHashIntBroadCast() {
        try (Connection connection = getConnection()) {
            String innoTable = "hash_test5";
            String ossTable = "hash_test5_oss";
            Statement statement = connection.createStatement();

            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id int NOT NULL,\n" +
                "    PRIMARY KEY (id)\n" +
                ")\n" +
                "broadcast\n", innoTable));
            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innoTable).append("('id') values");
            for (int i = 0; i < 999; i++) {
                insert.append("(").append(i * 10).append("),");
            }
            insert.append("(100000);");
            statement.executeUpdate(insert.toString());

            createOssTableLoadingFromInnodbTable(connection, ossTable, innoTable);

            List<String> sqls = new ArrayList<>();
            sqls.add("select check_sum(*) from %s");
            sqls.add("select check_sum(id) from %s where id < 100");
            compareSqls(sqls, connection, ossTable, innoTable);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testFullTypeByColumn() {
        try (Connection connection = getConnection()) {
            // for each column
            for (FullTypeSeparatedTestUtil.TypeItem typeItem : FullTypeSeparatedTestUtil.TypeItem.values()) {
                System.out.println("begin to check: " + typeItem.getKey());
                String columnName = typeItem.getKey();
                String innoTable = FullTypeSeparatedTestUtil.tableNameByColumn(columnName);
                String ossTable = "oss_" + innoTable;

                // gen data by column-specific type item.
                FullTypeSeparatedTestUtil.createInnodbTableWithData(connection, typeItem);

                // clone an oss-table from innodb-table.
                createOssTableLoadingFromInnodbTable(connection, ossTable, innoTable);

                // check
                compareSqls(ImmutableList.of("select check_sum(*) from %s"), connection, ossTable, innoTable);

                System.out.println("check ok: " + typeItem.getKey());
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOrcHashFullType() {
        try (Connection connection = getConnection()) {
            // full type test
            String innoTable = "hash_full_type";
            String ossTable = "oss_hash_full_type";
            FullTypeTestUtil.createInnodbTableWithData(connection, innoTable);
            createOssTableLoadingFromInnodbTable(connection, ossTable, innoTable);

            List<String> sqls = new ArrayList<>();
            sqls.add("select check_sum(*) from %s");
            sqls.add(
                "select check_sum(c_tinyint_1, c_double, c_decimal_pr, c_time_6, c_blob_tiny, c_enum) from %s");
            sqls.add("select check_sum(*) from %s where id > 0");
            sqls.add("select check_sum(*) from %s where id > 100301");
            compareSqls(sqls, connection, ossTable, innoTable);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void compareSqls(List<String> sqls, Connection connection, String ossTable, String innoTable)
        throws SQLException {
        for (String sql : sqls) {
            String ossPushedSql = String.format(sql, ossTable);
            String ossNotPushedSql = "/*+TDDL:cmd_extra(enable_cbo_push_agg=false)*/" + String.format(sql, ossTable);
            String innoSql = String.format(sql, innoTable);
            try (ResultSet ossPushedRs = JdbcUtil.executeQuerySuccess(connection, ossPushedSql);
                ResultSet ossNotPushedRs = JdbcUtil.executeQuerySuccess(connection, ossNotPushedSql);
                ResultSet innoRs = JdbcUtil.executeQuerySuccess(connection, innoSql)) {
                List<List<Object>> ossPushedList = JdbcUtil.getAllResult(ossPushedRs);
                List<List<Object>> ossNotPushedList = JdbcUtil.getAllResult(ossNotPushedRs);
                List<List<Object>> innoList = JdbcUtil.getAllResult(innoRs);

                assertWithMessage(ossPushedSql + " 与 " + ossNotPushedSql + " 结果不同")
                    .that(ossPushedList).containsExactlyElementsIn(ossNotPushedList);
                assertWithMessage(ossPushedSql + " 与 " + innoSql + " 结果不同")
                    .that(ossPushedList).containsExactlyElementsIn(innoList);
                assertWithMessage(ossNotPushedSql + " 与 " + innoSql + " 结果不同")
                    .that(ossNotPushedList).containsExactlyElementsIn(innoList);
            }
        }
    }

    public boolean createOssTableLoadingFromInnodbTable(Connection conn, String ossTableName, String innodbTableName)
        throws SQLException {
        Statement statement = conn.createStatement();
        return statement.execute(String.format(
            "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine ='%s' archive_mode = 'loading'",
            ossTableName, innodbTableName, engine.name()));
    }
}
