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
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptNewAccessor;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;


public class NewSequenceChangeTest extends BaseSequenceTestCase {

    private static final String CREATE_DATABASE = "create database if not exists %s mode='auto'";
    private static final String DROP_DATABASE = "drop database if exists %s";

    private static final String CREATE_SEQ = "create %s sequence %s";
    private static final String DROP_SEQ = "drop sequence %s";
    private static final String DROP_PHY_SEQ = "drop sequence if exists %s";

    private static final String SELECT_NEXTVAL = "select %s.nextval";
    private static final String SELECT_NEXTVAL_BATCH = "select %s.nextval from dual where count = %s";

    private static final String INFO_SCHEMA_SEQ = "select * from information_schema.sequences where schema_name='%s'";
    private static final String SHOW_SEQUENCES = "show sequences";

    private static final String CONVERT_SEQUENCES = "convert all sequences from %s to %s for %s";

    private static final int NUM_SEQ = 3;

    private final String dbName;
    private final String seqName;
    private final String fromType;
    private final String toType;

    private Connection conn;
    private Connection testConn;

    public NewSequenceChangeTest(String fromType, String toType) {
        this.dbName = "seq_change_test";
        this.seqName = "seq_";
        this.fromType = fromType;
        this.toType = toType;
    }

    @Parameterized.Parameters(name = "{index}: fromType={0}, toType={1}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {"NEW", "NEW"},
            {"NEW", "GROUP"},
            {"NEW", "TIME"},
            {"GROUP", "GROUP"},
            {"GROUP", "NEW"},
            {"GROUP", "TIME"},
            {"TIME", "NEW"},
            {"TIME", "GROUP"},
            {"TIME", "TIME"},
            {"ALL", "ALL"}
        });
    }

    @Before
    public void init() throws Exception {
        this.conn = getPolardbxConnection();
        dropPhySequences();
        dropDatabase(dbName);
        createDatabase(dbName);
        this.testConn = getPolardbxConnection(dbName);
    }

    @Test
    public void testSequenceChange() throws Exception {
        if (fromType.equals("ALL")) {
            return;
        }

        if (fromType.equals(toType)) {
            testSameTypeConversion();
            return;
        }

        createSequences(fromType);

        switch (fromType) {
        case "NEW":
            checkNextvalBatch(fromType, 1);
            checkNextvalBatch(fromType, 2);
            checkNextvalBatch(fromType, 10, 12);
            checkNextvalBatch(fromType, 10, 22);
            break;
        case "GROUP":
            checkNextvalBatch(fromType, 100001);
            checkNextvalBatch(fromType, 100002);
            checkNextvalBatch(fromType, 10, 100012);
            checkNextvalBatch(fromType, 10, 100022);
            break;
        case "TIME":
            // Cannot check the actual values, so ignore.
            break;
        }

        convertSequences(fromType, toType);

        Set<SeqInfo> expectedSeqInfos = new HashSet<>();

        switch (fromType) {
        case "NEW":
            switch (toType) {
            case "GROUP":
                expectedSeqInfos.addAll(buildSeqInfos(fromType, toType, 23));
                checkShowSequencesAndInfoSchema(expectedSeqInfos, false);
                checkNextvalBatch(fromType, 200001);
                checkNextvalBatch(fromType, 30, 200031);
                break;
            case "TIME":
                expectedSeqInfos.addAll(buildSeqInfos(fromType, toType, 0));
                checkShowSequencesAndInfoSchema(expectedSeqInfos, true);
                break;
            }
            break;
        case "GROUP":
            switch (toType) {
            case "NEW":
                expectedSeqInfos.addAll(buildSeqInfos(fromType, toType, 200000));
                checkShowSequencesAndInfoSchema(expectedSeqInfos, false);
                checkNextvalBatch(fromType, 200000);
                checkNextvalBatch(fromType, 30, 200030);
                break;
            case "TIME":
                expectedSeqInfos.addAll(buildSeqInfos(fromType, toType, 0));
                checkShowSequencesAndInfoSchema(expectedSeqInfos, true);
                break;
            }
            break;
        case "TIME":
            switch (toType) {
            case "NEW":
            case "GROUP":
                expectedSeqInfos.addAll(buildSeqInfos(fromType, toType, 0));
                checkShowSequencesAndInfoSchema(expectedSeqInfos, true);
                break;
            }
            break;
        }

        dropSequences(fromType);
    }

    private void testSameTypeConversion() {
        String sql = String.format(CONVERT_SEQUENCES, fromType, toType, dbName);
        JdbcUtil.executeUpdateFailed(testConn, sql, "Don't allow conversion between two same sequence types");

    }

    @Test
    public void testSequenceChangeAll() throws Exception {
        if (!fromType.equals("ALL")) {
            return;
        }

        createSequences("NEW");
        createSequences("GROUP");
        createSequences("TIME");

        checkNextvalBatch("NEW", 1);
        checkNextvalBatch("NEW", 20, 21);

        checkNextvalBatch("GROUP", 100001);
        checkNextvalBatch("GROUP", 20, 100021);

        convertSequences("GROUP", "NEW");

        Set<SeqInfo> expectedSeqInfos = buildSeqInfos("NEW", "NEW", 22);
        expectedSeqInfos.addAll(buildSeqInfos("GROUP", "NEW", 200000));
        expectedSeqInfos.addAll(buildSeqInfos("TIME", "TIME", 0));

        checkShowSequencesAndInfoSchema(expectedSeqInfos, false);

        checkNextvalBatch("NEW", 22);
        checkNextvalBatch("NEW", 20, 42);

        checkNextvalBatch("GROUP", 200000);
        checkNextvalBatch("GROUP", 20, 200020);

        convertSequences("NEW", "GROUP");

        expectedSeqInfos = buildSeqInfos("NEW", "GROUP", 43);
        expectedSeqInfos.addAll(buildSeqInfos("GROUP", "GROUP", 200021));
        expectedSeqInfos.addAll(buildSeqInfos("TIME", "TIME", 0));

        checkShowSequencesAndInfoSchema(expectedSeqInfos, false);

        checkNextvalBatch("NEW", 200001);
        checkNextvalBatch("NEW", 20, 200021);

        checkNextvalBatch("GROUP", 400001);
        checkNextvalBatch("GROUP", 20, 400021);

        dropSequences("NEW");
        dropSequences("GROUP");
        dropSequences("TIME");
    }

    private void createSequences(String seqType) {
        for (int i = 1; i <= NUM_SEQ; i++) {
            createSequence(seqType, seqName + seqType.toLowerCase() + i);
        }
    }

    private void dropSequences(String seqType) {
        for (int i = 1; i <= NUM_SEQ; i++) {
            dropSequence(seqName + seqType.toLowerCase() + i);
        }
    }

    private void dropPhySequences(String seqType) {
        for (int i = 1; i <= NUM_SEQ; i++) {
            dropPhySequence(seqName + seqType.toLowerCase() + i);
        }
    }

    private void checkNextvalBatch(String seqType, long expectedValue) throws Exception {
        for (int i = 1; i <= NUM_SEQ; i++) {
            checkNextval(seqName + seqType.toLowerCase() + i, expectedValue);
        }
    }

    private void checkNextvalBatch(String seqType, int count, long expectedMaxValue) throws Exception {
        for (int i = 1; i <= NUM_SEQ; i++) {
            checkNextval(seqName + seqType.toLowerCase() + i, count, expectedMaxValue);
        }
    }

    private void createSequence(String seqType, String seqName) {
        String sql = String.format(CREATE_SEQ, seqType, seqName);
        JdbcUtil.executeUpdateSuccess(testConn, sql);
    }

    public void dropSequence(String seqName) {
        String sql = String.format(DROP_SEQ, seqName);
        JdbcUtil.executeUpdateSuccess(testConn, sql);
    }

    public void dropPhySequence(String seqName) {
        String phySeqName = SequenceOptNewAccessor.genNameForNewSequence(dbName, seqName);
        String sql = String.format(DROP_PHY_SEQ, phySeqName);
        JdbcUtil.executeUpdateSuccess(getMetaConnection(), sql);
    }

    private void checkNextval(String seqName, long expectedValue) throws Exception {
        boolean matched = false;

        String sql = String.format(SELECT_NEXTVAL, seqName);
        try (Statement stmt = testConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long value = rs.getLong(1);
                matched = value == expectedValue;
            }
        }

        Assert.assertTrue(matched);
    }

    private void checkNextval(String seqName, int count, long expectedMaxValue) throws Exception {
        boolean matched = false;
        List<Long> values = new ArrayList<>();

        String sql = String.format(SELECT_NEXTVAL_BATCH, seqName, count);
        try (Statement stmt = testConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                values.add(rs.getLong(1));
            }
        }

        Optional<Long> max = values.stream().max(Long::compare);
        if (max.isPresent()) {
            matched = max.get() == expectedMaxValue;
        }

        Assert.assertTrue(matched);
    }

    private void convertSequences(String fromType, String toType) throws Exception {
        String sql = String.format(CONVERT_SEQUENCES, fromType, toType, dbName);
        try (Statement stmt = testConn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void checkShowSequencesAndInfoSchema(Set<SeqInfo> expectedSequences, boolean ignoreValueCheck)
        throws Exception {
        checkSequences(SHOW_SEQUENCES, expectedSequences, ignoreValueCheck);
        checkSequences(String.format(INFO_SCHEMA_SEQ, dbName), expectedSequences, ignoreValueCheck);
    }

    private void checkSequences(String sql, Set<SeqInfo> expectedSequences, boolean ignoreValueCheck) throws Exception {
        boolean matched = true;

        Set<SeqInfo> actualSequences = fetchSequences(sql, ignoreValueCheck);

        Assert.assertTrue(actualSequences.size() == expectedSequences.size(), "different sequence size");

        for (SeqInfo actual : actualSequences) {
            boolean existing = false;
            for (SeqInfo expected : expectedSequences) {
                if (actual.name.equalsIgnoreCase(expected.name)
                    && actual.type.equalsIgnoreCase(expected.type)
                    && (ignoreValueCheck || actual.value == expected.value)) {
                    existing = true;
                    break;
                }
            }
            matched &= existing;
        }

        StringBuilder buf = new StringBuilder();
        buf.append("Unmatched Sequence Result:\n");
        buf.append("Expected: " + expectedSequences).append("\n");
        buf.append("Actual: " + actualSequences);

        Assert.assertTrue(matched, buf.toString());
    }

    private Set<SeqInfo> fetchSequences(String sql, boolean ignoreValueCheck) throws Exception {
        Set<SeqInfo> seqInfos = new HashSet<>();
        try (Statement stmt = testConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                SeqInfo seqInfo = new SeqInfo();
                seqInfo.name = rs.getString("NAME");
                if (!ignoreValueCheck) {
                    try {
                        seqInfo.value = rs.getLong("VALUE");
                    } catch (SQLException e) {
                        if (e.getMessage().contains("Invalid value for getLong() - 'N/A'")) {
                            seqInfo.value = 0;
                        } else {
                            throw e;
                        }
                    }
                }
                seqInfo.type = rs.getString("TYPE");
                seqInfos.add(seqInfo);
            }
        }
        return seqInfos;
    }

    private Set<SeqInfo> buildSeqInfos(String fromType, String toType, long expectedValue) {
        Set<SeqInfo> seqInfos = new HashSet<>();
        for (int i = 1; i <= NUM_SEQ; i++) {
            SeqInfo seqInfo = new SeqInfo();
            seqInfo.name = seqName + fromType.toLowerCase() + i;
            seqInfo.value = expectedValue;
            seqInfo.type = toType;
            seqInfos.add(seqInfo);
        }
        return seqInfos;
    }

    private class SeqInfo {
        String name;
        long value;
        String type;

        @Override
        public String toString() {
            return "Name: " + name + ", Value: " + value + ", Type: " + type;
        }
    }

    private void createDatabase(String dbName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(CREATE_DATABASE, dbName));
        }
    }

    private void dropDatabase(String dbName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(DROP_DATABASE, dbName));
        }
    }

    private void dropPhySequences() {
        dropPhySequences("NEW");
        dropPhySequences("GROUP");
        dropPhySequences("TIME");
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @After
    public void destroy() throws Exception {
        dropDatabase(dbName);
        if (testConn != null) {
            try {
                testConn.close();
            } catch (SQLException ignored) {
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }
}
