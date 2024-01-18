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

package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NewSequenceTableTest extends BaseSequenceTestCase {

    private static final String CREATE_BASE_TABLE = "create table %s (age int)";
    private static final String CREATE_TABLE =
        "create table %s (id int not null primary key auto_increment %s, age int) %s";
    private static final String ALTER_TABLE = "alter table %s auto_increment=%s";

    private static final String INSERT_VALUE = "insert into %s(age) values(1)";
    private static StringBuilder INSERT_VALUES = new StringBuilder("insert into %s(age) values (1)");
    private static final String BATCH_INSERT = "insert into %s(age) values(?)";
    private static final String INSERT_SELECT = "insert into %s(age) select age from %s";

    private static final String SHOW_NEXTVAL = "show sequences where name='%s'";

    private static final String SELECT_ID = "select id from %s order by id";
    private static final String DELETE_TABLE = "delete from %s";

    protected static final String SET_FAIL_POINT = "set @%s='%s'";
    protected static final String SET_FP_CLEAR = "set @FP_CLEAR=true";

    private static final int BATCH_SIZE = 10;
    private static final int NUM_BATCH = 10;
    private static final int TOTAL_COUNT = BATCH_SIZE * NUM_BATCH;

    private static final long START_WITH = 1L;

    private final String seqType;
    private final String suffix;

    private final String simpleTableName;
    private final String tableName;

    private final String simpleBaseTableName;
    private final String baseTableName;

    private final String simpleSeqName;
    private final String seqName;

    public NewSequenceTableTest(String seqType, String schema, String suffix) {
        this.seqType = seqType;
        this.schema = schema;
        this.suffix = suffix;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";

        this.simpleTableName = randomTableName("test_new_seq", 4);
        this.tableName = schemaPrefix + simpleTableName;

        this.simpleBaseTableName = this.simpleTableName + "_base";
        this.baseTableName = schemaPrefix + simpleBaseTableName;

        this.simpleSeqName = "AUTO_SEQ_" + this.simpleTableName;
        this.seqName = schemaPrefix + this.simpleSeqName;
    }

    @Parameterized.Parameters(name = "{index}: seqType={0}, schema={1}, suffix={2}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {"NEW", "", ""},
            {"NEW", "", "broadcast"},
            {"NEW", "", "partition by key(id)"},
            {"NEW", PropertiesUtil.polardbXAutoDBName2(), ""},
            {"NEW", PropertiesUtil.polardbXAutoDBName2(), "broadcast"},
            {"NEW", PropertiesUtil.polardbXAutoDBName2(), "partition by key(id)"},
            {"SIMPLE", "", ""},
            {"SIMPLE", "", "broadcast"},
            {"SIMPLE", "", "partition by key(id)"},
            {"SIMPLE", PropertiesUtil.polardbXAutoDBName2(), ""},
            {"SIMPLE", PropertiesUtil.polardbXAutoDBName2(), "broadcast"},
            {"SIMPLE", PropertiesUtil.polardbXAutoDBName2(), "partition by key(id)"}
        });
    }

    static {
        for (int i = 1; i < TOTAL_COUNT; i++) {
            INSERT_VALUES.append(",(1)");
        }
    }

    @Before
    public void init() {
        dropTableIfExists(baseTableName);
        createBaseTable();
        loadBaseTable();
    }

    @After
    public void destroy() {
        dropTableIfExists(baseTableName);
    }

    @Test
    public void testSequenceWithTable() throws Exception {
        dropTableIfExists(tableName);
        createTable();

        checkSequenceWithTable(START_WITH);

        long newStartWith = 1000;
        alterTable(newStartWith);

        checkSequenceWithTable(newStartWith);

        dropTableIfExists(tableName);
    }

    @Test
    public void testSeqIdempotent() throws Exception {
        if (!seqType.equals("NEW")) {
            return;
        }

        dropTableIfExists(tableName);

        final String failPointKey = "FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION";

        try {
            injectException(failPointKey, true);
            createTable();
        } finally {
            injectException(failPointKey, false);
        }

        dropTableIfExists(tableName);
    }

    private void checkSequenceWithTable(long startWith) throws Exception {
        long expectedValue = startWith;
        checkShowNextval(expectedValue);

        insertValues(false);
        checkResult(1, expectedValue);
        checkShowNextval(++expectedValue);
        clearTableData();

        insertValues(true);
        checkResult(TOTAL_COUNT, expectedValue);
        expectedValue += TOTAL_COUNT;
        checkShowNextval(expectedValue);
        clearTableData();

        batchInsert();
        checkResult(TOTAL_COUNT, expectedValue);
        expectedValue += TOTAL_COUNT;
        checkShowNextval(expectedValue);
        clearTableData();

        insertSelect();
        checkResult(TOTAL_COUNT, expectedValue);
        expectedValue += TOTAL_COUNT;
        checkShowNextval(expectedValue);
        clearTableData();
    }

    private void insertValues(boolean multiple) {
        String sql = String.format(multiple ? INSERT_VALUES.toString() : INSERT_VALUE, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void batchInsert() throws Exception {
        String sql = String.format(BATCH_INSERT, tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            for (int i = 0; i < NUM_BATCH; i++) {
                for (int j = 0; j < BATCH_SIZE; j++) {
                    ps.setInt(1, 1);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    private void insertSelect() {
        String sql = String.format(INSERT_SELECT, tableName, baseTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void checkResult(int count, long firstExpectedValue) throws Exception {
        boolean matched = false;

        String sql = String.format(SELECT_ID, tableName);
        List<Long> values = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                values.add(rs.getLong(1));
            }
        }

        if (values.size() == count) {
            matched = true;
            for (long value : values) {
                matched &= value == firstExpectedValue++;
            }
        }

        Assert.assertTrue(matched);
    }

    private void createTable() {
        String type = TStringUtil.equalsIgnoreCase(seqType, "NEW") ? "" : " BY " + seqType;
        String sql = String.format(CREATE_TABLE, tableName, type, suffix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void alterTable(long startWith) {
        String sql = String.format(ALTER_TABLE, tableName, startWith);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void injectException(String failPointKey, boolean enabled) throws SQLException {
        String sql = enabled ? String.format(SET_FAIL_POINT, failPointKey, "TRUE") : SET_FP_CLEAR;
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.executeUpdate();
        }
    }

    private void clearTableData() {
        String sql = String.format(DELETE_TABLE, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void checkShowNextval(long expectedValue) throws SQLException {
        if (TStringUtil.isEmpty(suffix)) {
            // Single table
            return;
        }
        boolean matched = false;
        Connection conn = TStringUtil.isBlank(schema) ? tddlConnection : tddlConnection2;
        String sql = String.format(SHOW_NEXTVAL, simpleSeqName);
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long value = rs.getLong("VALUE");
                String type = rs.getString("TYPE");
                matched = value == expectedValue && TStringUtil.equals(type, seqType);
            }
        }
        Assert.assertTrue(matched);
    }

    private void createBaseTable() {
        String sql = String.format(CREATE_BASE_TABLE, baseTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void loadBaseTable() {
        String sql = String.format(INSERT_VALUES.toString(), baseTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
