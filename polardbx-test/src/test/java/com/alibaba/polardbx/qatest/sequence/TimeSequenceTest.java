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

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.common.IdGenerator.MAX_NUM_OF_IDS_PER_RANGE;

/**
 * Created by chensr on 2018-03-13 测试 Time-based Sequence 批量插入
 */
@NotThreadSafe

public class TimeSequenceTest extends BaseSequenceTestCase {

    private String targetTable;
    private String sourceTable;

    private static StringBuilder sqlInsertValues = new StringBuilder("insert into %s(c2) values (1)");
    private static final String sqlInsertSelect = "insert into %s(c2) select c2 from %s";
    private static final String sqlTruncateTable = "truncate table %s";

    private static final int numOfIds = 10000;
    private static final int insertSelectBatchSize = 100;

    public TimeSequenceTest(String schema) {
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.targetTable = schemaPrefix + randomTableName("tab_batch_time_seq", 4);
        this.sourceTable = schemaPrefix + randomTableName("tab_select_source", 4);
    }

    @Parameterized.Parameters(name = "{index}:schema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {{""}, {PropertiesUtil.polardbXShardingDBName2()}});
    }

    static {
        for (int i = 1; i < numOfIds; i++) {
            sqlInsertValues.append(",(1)");
        }
    }

    @Before
    public void init() {
        String sqlCreateTarget =
            "create table if not exists %s (c1 bigint not null primary key auto_increment by time, c2 int) dbpartition by hash(c1)";
        String sqlCreateSource = "create table if not exists %s (c2 int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateTarget, targetTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateSource, sourceTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlInsertValues.toString(), sourceTable));
    }

    @After
    public void destroy() {
        dropTableIfExists(targetTable);
        dropTableIfExists(sourceTable);
    }

    @Test
    public void testInsertValues() {
        // Truncate the target table first
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlTruncateTable, targetTable));
        // Insert 5000 values. The number is greater than max range size 4096.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlInsertValues.toString(), targetTable));
        // Start validating the time-based sequence ids.
        validateIds(MAX_NUM_OF_IDS_PER_RANGE);
    }

    @Test
    public void testInsertSelect() {
        // Truncate the target table first
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlTruncateTable, targetTable));
        // Insert from the source table. InsertSelect executes once per 100 rows.
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlInsertSelect, targetTable, sourceTable));
        // Start validating the time-based sequence ids.
        validateIds(insertSelectBatchSize);
    }

    private void validateIds(int batchSize) {
        List<Long> results = queryTargetTable();

        Assert.assertTrue(results.size() == numOfIds);

        int numOfRanges = numOfIds / batchSize;

        long lastTimestamp, currentTimestamp;
        long lastSequence, CurrentSequence;
        long timeSequenceId;

        for (int i = 0; i < numOfRanges; i++) {
            timeSequenceId = results.get(i * batchSize);
            lastTimestamp = IdGenerator.extractTimestamp(timeSequenceId);
            lastSequence = IdGenerator.extractSequence(timeSequenceId);
            for (int j = 1; j < batchSize; j++) {
                timeSequenceId = results.get(i * batchSize + j);
                // Validate timestamp
                currentTimestamp = IdGenerator.extractTimestamp(timeSequenceId);
                Assert.assertTrue(currentTimestamp == lastTimestamp);
                lastTimestamp = currentTimestamp;
                // Validate sequence
                CurrentSequence = IdGenerator.extractSequence(timeSequenceId);
                Assert.assertTrue(CurrentSequence == (lastSequence + 1));
                lastSequence = CurrentSequence;
            }
        }
    }

    private List<Long> queryTargetTable() {
        String sqlQuery = "select c1 from %s order by c1";
        List<Long> results = new ArrayList<>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = tddlConnection.prepareStatement(String.format(sqlQuery, targetTable));
            rs = stmt.executeQuery();
            while (rs.next()) {
                results.add(rs.getLong(1));
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
        }
        return results;
    }

}
