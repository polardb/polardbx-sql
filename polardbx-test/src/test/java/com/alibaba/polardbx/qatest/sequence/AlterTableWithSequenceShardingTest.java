package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.entity.TestSequence;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by xiaowen.guoxw on 16-12-12.
 */

public class AlterTableWithSequenceShardingTest extends BaseSequenceTestCase {

    private String simpleTableName;
    private String tableName;
    private String sqlPostFix = "";
    private String seqType;

    public AlterTableWithSequenceShardingTest(String seqType, String sqlPostFix, String schema) {
        this.seqType = seqType;
        this.sqlPostFix = sqlPostFix;
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.simpleTableName = randomTableName("alter_seq_test2", 4);
        this.tableName = schemaPrefix + simpleTableName;
    }

    @Parameterized.Parameters(name = "{index}:seqType={0}, sqlPostFix={1}, schema={2}")
    public static List<String[]> prepareData() {
        String[][] postFix = {
            {"", "", ""},
            {"", "dbpartition by hash(id)", ""},
            {"", "broadcast", ""},

            {"by group", "", ""},
            {"by group", "dbpartition by hash(id)", ""},
            {"by group", "broadcast", ""},

            {"by time", "", ""},
            {"by time", "dbpartition by hash(id)", ""},
            {"by time", "broadcast", ""},

            {"", "", PropertiesUtil.polardbXShardingDBName2()},
            {"", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {"", "broadcast", PropertiesUtil.polardbXShardingDBName2()},

            {"by group", "", PropertiesUtil.polardbXShardingDBName2()},
            {"by group", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {"by group", "broadcast", PropertiesUtil.polardbXShardingDBName2()},

            {"by time", "", PropertiesUtil.polardbXShardingDBName2()},
            {"by time", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {"by time", "broadcast", PropertiesUtil.polardbXShardingDBName2()}
        };
        return Arrays.asList(postFix);
    }

    @Before
    public void cleanTable() {
        dropTableIfExists(tableName);
    }

    @After
    public void afterCleanTable() {
        dropTableIfExists(tableName);
    }

    @Test
    public void testAlterSeqAutoIncrementStartWith() {

        String sql = String.format(
            "create table %s (auto_id bigint not null auto_increment %s primary key, id int , name varchar(20)) %s",
            tableName,
            seqType,
            sqlPostFix);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String insert_sql = String.format("insert into %s (id, name) values (1, 'abc')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        sql = String.format("alter table %s auto_increment = 40", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        if (seqType.toLowerCase().contains("group")) {
            assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
                "auto_id",
                tddlConnection)).contains(200001L);
        }
    }

    @Test
    public void testAlterSeqAutoIncrementStartWith2() {
        String sql = String.format(
            "create table %s (auto_id int not null auto_increment primary key, id int , name varchar(20)) dbpartition by hash(id)",

            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s auto_increment = 40", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String insert_sql = String.format("insert into %s (id, name) values (1, 'abc')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        if (seqType.toLowerCase().contains("group")) {
            assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
                "auto_id",
                tddlConnection)).contains(200001L);
        }
    }

    @Test
    public void testAlterSeqChangeColumnOnSingleTable() {
        if (!seqType.isEmpty() || !sqlPostFix.isEmpty()) {
            return;
        }

        String sql = String.format(
            "create table %s (auto_id int not null auto_increment primary key, id int , name varchar(20))",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column auto_id auto_id bigint not null auto_increment", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence seq = showSequence(schemaPrefix + "AUTO_SEQ_" + simpleTableName);
        Assert.assertTrue("Unexpected: a sequence is created and associated with the single table", seq == null);
    }

    @Test
    public void testAlterSeqAutoIncrementWithGsi() {
        if (!TStringUtil.isEmpty(schema)) {
            // Adding global index on other schema is forbidden
            return;
        }
        String sql = String.format("create table %s (`id` int(10) unsigned not null auto_increment primary key, "
                + "`k` int(10) unsigned not null default '0', `c` char(120) not null default '', index `k_1` (`k`))",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s auto_increment = 100", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

}
