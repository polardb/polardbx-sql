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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.entity.NewSequence;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.validator.DataValidator.isIndexExist;
import static com.google.common.truth.Truth.assertThat;

/**
 * @author shicai.xsc 2019/4/27 下午9:32
 * @since 5.0.0.0
 */
@Ignore
public class CrossSchemaDdlTest extends DDLBaseNewDBTestCase {

    private String schema = "`" + PropertiesUtil.polardbXShardingDBName2() + "`";
    private String schemaPrefix = schema + ".";
    private String simpleTableName = "cross_db";
    private String tableDefinition = "(id int, value int)";
    //private String[] seqTypes = new String[] {"", "simple", "simple with cache", "group", "time"};
    private String[] seqTypes = new String[] {"", "simple", "group", "time"};
    private String sqlPostFix = "dbpartition by hash(id)";
    private static final String ALLOW_ALTER_GSI_INDIRECTLY_HINT =
        "/*+TDDL:cmd_extra(ALLOW_ALTER_GSI_INDIRECTLY=true)*/";

    private String tableName;

    @Before
    public void initCrossSchemaName() {
        this.tableName = String.format("%s.`%s`", schema, simpleTableName);
    }

    @Test
    public void testTableDdl() throws SQLException {
        dropTableIfExists(tableName);

        // 1. Create table
        String createSql = String.format("create table %s %s", tableName, tableDefinition);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String insertSql = String.format("insert into %s values(1,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        String selectSql = String.format("select * from %s", tableName);
        ResultSet res = JdbcUtil.executeQuerySuccess(tddlConnection, selectSql);
        Assert.assertTrue(res.next());

        // 2. Alter table
        String alterSql = String.format("alter table %s add column age int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterSql);

        insertSql = String.format("insert into %s values(2,2,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        res = JdbcUtil.executeQuerySuccess(tddlConnection, selectSql);
        Assert.assertTrue(res.next());

        // 3. Rename table
        String newTableName = tableName.replace("cross_db", "cross_db_1");
        String renameSql =
            String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + "rename table %s to %s", tableName, newTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, renameSql);

        selectSql = String.format("select * from %s", newTableName);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, selectSql);
        Assert.assertTrue(res.next());

        // 4. Truncate table
        String truncateSql = String.format("truncate table %s", newTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, truncateSql);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, selectSql);
        Assert.assertFalse(res.next());

        // 5. Drop table
        dropTableIfExists(newTableName);
    }

    @Test
    public void testIndexDdl() {
        dropTableIfExists(tableName);

        String createSql = String.format("create table %s %s", tableName, tableDefinition);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // 1. Create index
        String indexName = "index" + "_1";
        String createIndexSql = "create unique index " + indexName + " on " + tableName + " (id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndexSql);

        // 2. Show index
        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));

        // 3. Drop index
        clearIndex(tableName, indexName);
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);
    }

    @Test
    public void testSequenceDdl() throws Exception {
        for (String seqType : seqTypes) {
            // Create sequence
            String seqName = schemaPrefix + "cross_seq";
            dropSeqence(seqName);
            String createSql = String.format("create %s sequence %s ", seqType, seqName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
            assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
            simpleCheckSequence(seqName, seqType);

            // Alter sequence
            NewSequence sequenceBefore = showSequence(seqName);
            String alterSeqSql = String.format("alter sequence %s maxvalue 100", seqName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, alterSeqSql);
            NewSequence sequenceAfter = showSequence(seqName);
            assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
            assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
            assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());
            if (!isSpecialSequence(seqType)) {
                assertThat(sequenceAfter.getMaxValue()).isEqualTo(100);
            }
            getSequenceNextVal(seqName);
            simpleCheckSequence(seqName, seqType);

            // Rename sequence
            String seqNameNew = schemaPrefix + "cross_new_seq";
            dropSeqence(seqNameNew);
            String renameSql = String.format("rename sequence %s to %s", seqName, seqNameNew);
            JdbcUtil.executeUpdateSuccess(tddlConnection, renameSql);
            sequenceAfter = showSequence(seqNameNew);
            assertThat(sequenceAfter).isNotNull();
            getSequenceNextVal(seqNameNew);
            simpleCheckSequence(seqNameNew, seqType);

            // Drop sequence
            dropSeqence(seqNameNew);
            sequenceAfter = showSequence(seqNameNew);
            Assert.assertNull(sequenceAfter);
        }
    }

    @Test
    public void testSequenceByCreateTable() {
        for (String seqType : seqTypes) {
            dropTableIfExists(tableName);

            // Create table with new sequence
            String createTable = String.format(
                "create table %s (auto_id bigint primary key not null auto_increment %s, name varchar(20)) dbpartition by hash(auto_id)",
                tableName,
                StringUtils.isBlank(seqType) ? seqType : "by " + seqType);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

            String inserSql = "insert into " + tableName + " (name) values ('one'), ('two')";
            JdbcUtil.executeUpdateSuccess(tddlConnection, inserSql);

            checkAutoSeqExists(tableName, seqType);
        }
    }

    @Test
    public void testClearSeqCache() throws Exception {
        String seqName = schemaPrefix + "simpleSeq";

        dropSeqence(seqName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "create simple sequence " + seqName);
        simpleCheckSequence(seqName, "simple");

        long valueBeforeClear = getSequenceNextVal(seqName);

        // simpleCheckSequence already got two values 1 and 2
        assertThat(valueBeforeClear).isEqualTo(3);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "clear sequence cache for " + seqName);
        long valueAfterClear = getSequenceNextVal(seqName);

        // Still next value after clear sequence cache since simle sequence
        // doesn't cache any value at all.
        assertThat(valueAfterClear).isEqualTo(4);
    }

    @Test
    public void testDal() throws Exception {
        dropTableIfExists(tableName);

        // Show tables
        String showTableSql = String.format("show tables in %s", schema);
        ResultSet res = JdbcUtil.executeQuerySuccess(tddlConnection, showTableSql);
        checkResExists(res, 1, simpleTableName, false);

        String createSql = String.format("create table %s (id int, value int) dbpartition by hash(id)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, showTableSql);
        checkResExists(res, 1, simpleTableName, true);

        // Show create table
        String showCreateTableSql = String.format("show create table %s", tableName);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(res.next());
        String createTableString = res.getString(2);
        Assert.assertTrue(createTableString.contains(simpleTableName));

        // Show table status
        String showTableStatusSql = String.format("show table status from %s", schema);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, showTableStatusSql);
        checkResExists(res, 1, simpleTableName, true);

        // show rule from table
        String showRuleSql = String.format("show rule from %s", tableName);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, showRuleSql);
        checkResExists(res, 2, simpleTableName, true);

        // Show partitions from table
        String showPartitionsSql = String.format("show partitions from %s", tableName);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, showPartitionsSql);
        checkResExists(res, 1, "id", true);

        // Show topology from table
        String showTopologySql = String.format("show topology from %s", tableName);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, showTopologySql);
        checkResExists(res, 3, simpleTableName, true);

        // Show index
        String showIndexSql = String.format("show index from %s", tableName);
        res = JdbcUtil.executeQuerySuccess(tddlConnection, showIndexSql);
        checkResExists(res, 1, simpleTableName, true);

        dropTableIfExists(tableName);
    }

    private void checkAutoSeqExists(String table, String seqType) {
        try {
            String select_sql = String.format("select * from %s where auto_id > 0", table);
            ResultSet res = JdbcUtil.executeQuerySuccess(tddlConnection, select_sql);
            Assert.assertTrue(res.next());

            String seqName = schemaPrefix + "AUTO_SEQ_" + getSimpleTableName(table).replaceAll("`", "");
            if (StringUtils.isBlank(seqType)) {
                if (StringUtils.isBlank(sqlPostFix)) {
                    // for non-sharding && non-broadcast table, no sequencewould
                    // be
                    // create
                    assertNoSequenceExist(seqName);
                } else {
                    // by default: group
                    simpleCheckSequence(seqName, "group");
                }
            } else {
                simpleCheckSequence(seqName, seqType);
            }
        } catch (Exception e) {
            logger.error(e);
            Assert.fail(e.getMessage());
        }
    }

    private void checkResExists(ResultSet res, int column, String resultToCheck, boolean checkExist) {
        boolean exist = false;
        try {
            while (res.next()) {
                if (res.getString(column).toLowerCase().contains(resultToCheck.toLowerCase())) {
                    exist = true;
                    break;
                }
            }
        } catch (Exception e) {
            Assert.fail();
        }

        if (checkExist) {
            Assert.assertTrue(exist);
        } else {
            Assert.assertFalse(exist);
        }
    }
}
