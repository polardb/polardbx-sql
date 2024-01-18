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
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NewSequenceCnTest extends BaseSequenceTestCase {

    private static final String CREATE_SEQ = "create %s sequence %s ";
    private static final String CREATE_SEQ_START_WITH = CREATE_SEQ + " start with %s";
    private static final String CREATE_SEQ_FULL = CREATE_SEQ_START_WITH + " increment by %s maxvalue %s %s";

    private static final String ALTER_SEQ = "alter sequence %s ";
    private static final String ALTER_SEQ_START_WITH = ALTER_SEQ + " start with %s";

    private static final String RENAME_SEQ = "rename sequence %s to %s";
    private static final String DROP_SEQ = "drop sequence %s";

    private static final String SELECT_NEXTVAL = "select %s.nextval";
    private static final String SELECT_NEXTVAL_BATCH = "select %s.nextval from dual where count = %s";

    private static final String SELECT_CURRVAL = "select %s.currval";

    private static final String SHOW_NEXTVAL = "show sequences where name='%s'";

    private final String simpleSeqName;
    private final String seqName;
    private final String newSeqName;
    private final String seqType;

    public NewSequenceCnTest(String seqType, String schema) {
        this.seqType = seqType;
        this.schema = schema;
        this.schemaPrefix = TStringUtil.isBlank(schema) ? "" : schema + ".";
        this.simpleSeqName = randomTableName("new_seq", 4);
        this.seqName = this.schemaPrefix + simpleSeqName;
        this.newSeqName = this.seqName + "_new";
    }

    @Parameterized.Parameters(name = "{index}: seqType={0}, schema={1}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {"NEW", ""},
            {"NEW", PropertiesUtil.polardbXAutoDBName2()},
            {"SIMPLE", ""},
            {"SIMPLE", PropertiesUtil.polardbXAutoDBName2()}
        });
    }

    @Test
    public void testSequenceDefault() throws Exception {
        testSequence(0, 1000);
    }

    @Test
    public void testSequenceStartWith() throws Exception {
        testSequence(100, 2000);
    }

    @Test
    public void testShowNextval() throws Exception {
        dropSequence();
        createSequence();

        checkNextval(1);
        checkCurrval(1);
        checkNextval(2);
        checkCurrval(2);

        checkShowNextval(3);
        checkNextval(3);
        checkCurrval(3);

        checkShowNextval(4);
        checkNextval(4);
        checkCurrval(4);

        alterSequence(100);

        checkShowNextval(100);
        checkNextval(100);
        checkCurrval(100);

        checkShowNextval(101);
        checkNextval(101);
        checkCurrval(101);

        dropSequence();
    }

    @Test
    public void testSkipToLessValueThenConsecutive() throws Exception {
        dropSequence();
        createSequence();

        checkNextval(1);
        checkCurrval(1);
        checkNextval(2);
        checkCurrval(2);

        alterSequence(200);

        checkShowNextval(200);
        checkNextval(200);
        checkCurrval(200);

        checkShowNextval(201);
        checkNextval(201);
        checkCurrval(201);

        checkShowNextval(202);
        checkNextval(202);
        checkCurrval(202);

        alterSequence(100);

        checkShowNextval(100);
        checkNextval(100);
        checkCurrval(100);

        checkShowNextval(101);
        checkNextval(101);
        checkCurrval(101);

        alterSequence(50);

        checkShowNextval(50);
        checkNextval(50);
        checkCurrval(50);

        checkShowNextval(51);
        checkNextval(51);
        checkCurrval(51);

        dropSequence();
    }

    @Test
    public void testMaxValueAssignment() throws Exception {
        dropSequence();

        createSequence(9223372036854775806L);

        checkShowNextval(9223372036854775806L);
        checkNextval(9223372036854775806L);
        checkCurrval(9223372036854775806L);

        checkShowNextval(9223372036854775807L);
        checkNextval(9223372036854775807L);
        checkCurrval(9223372036854775807L);

        try {
            checkNextvalRunOut();
        } finally {
            dropSequence();
        }

        createSequence(9223372036854775807L);

        checkShowNextval(9223372036854775807L);
        checkNextval(9223372036854775807L);
        checkCurrval(9223372036854775807L);

        try {
            checkNextvalRunOut();
        } finally {
            dropSequence();
        }
    }

    @Test
    public void testAlterSequence() throws Exception {
        dropSequence();

        try {
            createSequence(100, 2, 110, true);

            for (int i = 0; i < 50; i++) {
                checkNextval(100 + (i % 6) * 2);
            }

            alterSequence("nocycle");

            for (int i = 0; i < 4; i++) {
                checkNextval(104 + (i % 6) * 2);
            }

            checkNextvalRunOut();

            alterSequence("cycle");

            for (int i = 0; i < 48; i++) {
                checkNextval(100 + (i % 6) * 2);
            }

            alterSequence("increment by 3");

            for (int i = 0; i < 46; i++) {
                checkNextval(100 + (i % 4) * 3);
            }

            alterSequence("maxvalue 120");

            for (int i = 0; i < 5; i++) {
                checkNextval(106 + (i % 7) * 3);
            }

            for (int i = 0; i < 48; i++) {
                checkNextval(100 + (i % 7) * 3);
            }

            alterSequence("start with 110");

            for (int i = 0; i < 47; i++) {
                checkNextval(110 + (i % 4) * 3);
            }

            alterSequence("increment by 1");

            for (int i = 0; i < 4; i++) {
                checkNextval(117 + (i % 11) * 1);
            }

            for (int i = 0; i < 110; i++) {
                checkNextval(110 + (i % 11) * 1);
            }

            alterSequence("maxvalue 1000000000");

            alterSequence("start with 10000");

            for (int i = 0; i < 1000; i++) {
                checkNextval(10000 + i);
            }
        } finally {
            dropSequence();
        }
    }

    private void testSequence(long startWith1, long startWith2)
        throws Exception {
        dropSequence();
        createSequence(startWith1);

        checkSequence(startWith1 <= 0 ? 1 : startWith1, 5, 10);

        alterSequence(startWith2);

        checkSequence(startWith2, 6, 8);

        renameSequence();

        dropSequence();
    }

    private void checkSequence(long startWith, int count1, int count2) throws Exception {
        long expectedValue = startWith;

        checkShowNextval(expectedValue);

        checkNextval(expectedValue);
        checkCurrval(expectedValue);

        checkNextval(++expectedValue);
        checkCurrval(expectedValue);

        checkShowNextval(expectedValue + 1);

        // Batch syntax doesn't work with cross schema.
        if (TStringUtil.isEmpty(schema)) {
            long[] expectedValues = genExpectedValues(expectedValue, count1);
            checkNextval(count1, expectedValues);
            expectedValue += count1;
            checkCurrval(expectedValue);

            expectedValues = genExpectedValues(expectedValue, count2);
            checkNextval(count2, expectedValues);
            expectedValue += count2;
            checkCurrval(expectedValue);
        }

        checkShowNextval(expectedValue + 1);
    }

    private void createSequence() {
        createSequence(0);
    }

    private void createSequence(long startWith) {
        String type = TStringUtil.equalsIgnoreCase(seqType, "NEW") ? "" : seqType;
        String sql = startWith > 0 ? String.format(CREATE_SEQ_START_WITH, type, seqName, startWith) :
            String.format(CREATE_SEQ, type, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void createSequence(long startWith, long incrementBy, long maxValue, boolean isCycle) {
        String type = TStringUtil.equalsIgnoreCase(seqType, "NEW") ? "" : seqType;
        String sql = String.format(CREATE_SEQ_FULL, type, seqName, startWith, incrementBy, maxValue,
            isCycle ? "cycle" : "nocycle");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void alterSequence(long startWith) {
        String sql = String.format(ALTER_SEQ_START_WITH, seqName, startWith);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void alterSequence(String clause) {
        String sql = String.format(ALTER_SEQ, seqName) + clause;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void renameSequence() {
        String sql = String.format(RENAME_SEQ, seqName, newSeqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void dropSequence() {
        String sql = String.format(DROP_SEQ, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(DROP_SEQ, newSeqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void checkNextval(long expectedValue) throws Exception {
        checkValue(SELECT_NEXTVAL, expectedValue);
    }

    private void checkCurrval(long expectedValue) throws Exception {
        checkValue(SELECT_CURRVAL, expectedValue);
    }

    private void checkValue(String sql, long expectedValue) throws Exception {
        boolean matched = false;
        sql = String.format(sql, seqName);
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long value = rs.getLong(1);
                matched = value == expectedValue;
            }
        }
        Assert.assertTrue(matched);
    }

    private void checkNextvalRunOut() {
        String sql = String.format(SELECT_NEXTVAL, seqName);
        String errMsg = TStringUtil.equals(seqType, "NEW") ? "has run out" : "exceeds maximum value allowed";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, errMsg);
    }

    private void checkNextval(int count, long[] expectedValues) throws Exception {
        boolean matched = false;

        String sql = String.format(SELECT_NEXTVAL_BATCH, seqName, count);
        List<Long> values = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                values.add(rs.getLong(1));
            }
        }

        if (values.size() == expectedValues.length) {
            matched = true;
            for (int i = 0; i < values.size(); i++) {
                matched &= values.get(i) == expectedValues[i];
            }
        }

        Assert.assertTrue(matched);
    }

    private void checkShowNextval(long expectedValue) throws SQLException {
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

    private long[] genExpectedValues(long base, int count) {
        long[] expectedValues = new long[count];
        for (int i = 0; i < count; i++) {
            expectedValues[i] = ++base;
        }
        return expectedValues;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
