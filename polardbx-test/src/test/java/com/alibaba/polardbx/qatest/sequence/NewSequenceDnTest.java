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
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class NewSequenceDnTest extends BaseSequenceTestCase {

    private static final String CREATE_SEQ = "create sequence %s nocache";
    private static final String CREATE_SEQ_START_WITH = CREATE_SEQ + " start with %s";
    private static final String DROP_SEQ = "drop sequence if exists %s";

    private static final String SELECT_NEXTVAL = "select nextval(%s)";
    private static final String SELECT_NEXTVAL_BATCH = "select nextval(%s, %s)";
    private static final String SELECT_NEXTVAL_SKIP = "select nextval_skip(%s, %s)";
    private static final String SELECT_NEXTVAL_SHOW = "select nextval_show(%s)";

    private Connection metaDbConn;
    private final String seqName;

    public NewSequenceDnTest() {
        this.seqName = "new_ali_seq";
    }

    @Before
    public void init() {
        this.metaDbConn = getMetaConnection();
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
        checkNextval(2);

        checkShowNextval(3);
        checkNextval(3);

        checkShowNextval(4);
        checkNextval(4);

        skipSequence(100);

        checkShowNextval(101);
        checkNextval(101);

        checkShowNextval(102);
        checkNextval(102);

        dropSequence();
    }

    @Test
    public void testSkipToLessValueThenConsecutive() throws Exception {
        dropSequence();
        createSequence();

        checkNextval(1);
        checkNextval(2);

        skipSequence(200);

        checkShowNextval(201);
        checkNextval(201);

        checkShowNextval(202);
        checkNextval(202);

        checkShowNextval(203);
        checkNextval(203);

        skipSequenceWithWarning(100, "");

        checkShowNextval(204);
        checkNextval(204);

        checkShowNextval(205);
        checkNextval(205);

        skipSequenceWithWarning(50, "");

        checkShowNextval(206);
        checkNextval(206);

        checkShowNextval(207);
        checkNextval(207);

        dropSequence();
    }

    @Test
    public void testMaxValueAssignment() throws Exception {
        dropSequence();

        createSequence(9223372036854775806L);

        checkShowNextval(9223372036854775806L);
        checkNextval(9223372036854775806L);

        checkShowNextval(9223372036854775807L);
        checkNextval(9223372036854775807L);

        checkNextvalRunOut();

        dropSequence();

        createSequence(9223372036854775807L);

        checkShowNextval(9223372036854775807L);
        checkNextval(9223372036854775807L);

        checkNextvalRunOut();

        dropSequence();
    }

    private void testSequence(long startWith1, long startWith2)
        throws Exception {
        dropSequence();
        createSequence(startWith1);

        checkSequence(startWith1 <= 0 ? 1 : startWith1, 5, 10);

        skipSequence(startWith2);

        checkSequence(startWith2 + 1, 6, 8);

        dropSequence();
    }

    private void checkSequence(long startWith, int count1, int count2) throws Exception {
        long expectedValue = startWith;

        checkNextval(expectedValue);

        checkNextval(++expectedValue);

        // Batch syntax doesn't work with cross schema.
        long[] expectedValues = genExpectedValues(++expectedValue, count1);
        checkNextval(count1, expectedValues);

        expectedValue += count1;

        expectedValues = genExpectedValues(expectedValue, count2);
        checkNextval(count2, expectedValues);
    }

    private void createSequence() {
        createSequence(0);
    }

    private void createSequence(long startWith) {
        String sql = startWith > 0 ? String.format(CREATE_SEQ_START_WITH, seqName, startWith) :
            String.format(CREATE_SEQ, seqName);
        JdbcUtil.executeUpdateSuccess(metaDbConn, sql);
    }

    private void skipSequence(long skippedTo) {
        String sql = String.format(SELECT_NEXTVAL_SKIP, seqName, skippedTo);
        JdbcUtil.executeUpdateSuccess(metaDbConn, sql);
    }

    private void skipSequenceWithWarning(long skippedTo, String warningKeyword) {
        String sql = String.format(SELECT_NEXTVAL_SKIP, seqName, skippedTo);
        JdbcUtil.executeUpdateSuccessWithWarning(metaDbConn, sql, warningKeyword);
    }

    private void dropSequence() {
        String sql = String.format(DROP_SEQ, seqName);
        JdbcUtil.executeUpdateSuccess(metaDbConn, sql);
    }

    private void checkShowNextval(long expectedValue) throws Exception {
        checkValue(SELECT_NEXTVAL_SHOW, expectedValue);
    }

    private void checkNextval(long expectedValue) throws Exception {
        checkValue(SELECT_NEXTVAL, expectedValue);
    }

    private void checkNextvalRunOut() {
        String sql = String.format(SELECT_NEXTVAL, seqName);
        JdbcUtil.executeUpdateFailed(metaDbConn, sql, "has run out");
    }

    private void checkValue(String sql, long expectedValue) throws Exception {
        boolean matched = false;
        sql = String.format(sql, seqName);
        try (Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long value = rs.getLong(1);
                matched = value == expectedValue;
            }
        }
        Assert.assertTrue(matched);
    }

    private void checkNextval(int count, long[] expectedValues) throws Exception {
        boolean matched = true;

        long startValue = 0;
        String sql = String.format(SELECT_NEXTVAL_BATCH, seqName, count);
        try (Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                startValue = rs.getLong(1);
            }
        }

        for (int i = 0; i < count; i++) {
            matched &= startValue++ == expectedValues[i];
        }

        Assert.assertTrue(matched);
    }

    private long[] genExpectedValues(long base, int count) {
        long[] expectedValues = new long[count];
        for (int i = 0; i < count; i++) {
            expectedValues[i] = base++;
        }
        return expectedValues;
    }

    @After
    public void destroy() {
        if (this.metaDbConn != null) {
            try {
                this.metaDbConn.close();
            } catch (SQLException ignored) {
            }
        }
    }

}
