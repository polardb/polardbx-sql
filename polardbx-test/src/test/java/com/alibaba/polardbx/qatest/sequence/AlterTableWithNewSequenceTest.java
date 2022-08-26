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

import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by xiaowen.guoxw on 16-12-12.
 */

public class AlterTableWithNewSequenceTest extends BaseSequenceTestCase {

    private String tableName;
    private String sqlPostFix = "";
    private String seqType;

    public AlterTableWithNewSequenceTest(String seqType, String sqlPostFix, String schema) {
        this.seqType = seqType;
        this.sqlPostFix = sqlPostFix;
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.tableName = schemaPrefix + randomTableName("alter_seq_test2", 4);
    }

    @Parameterized.Parameters(name = "{index}:seqType={0}, sqlPostFix={1}, schema={2}")
    public static List<String[]> prepareData() {
        String[][] postFix = {
            {"", "", ""},
            {"", "dbpartition by hash(id)", ""},
            {"", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
            {"", "tbpartition by hash(id) tbpartitions 2", ""},
            {"", "broadcast", ""},
            {"by simple", "dbpartition by hash(id)", ""},
            {"by simple", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
            {"by simple", "tbpartition by hash(id) tbpartitions 2", ""},
            {"by simple", "broadcast", ""},
            //{ "by simple with cache", "dbpartition by hash(id)", "" },
            //{ "by simple with cache", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", "" },
            //{ "by simple with cache", "tbpartition by hash(id) tbpartitions 2", "" },
            //{ "by simple with cache", "broadcast", "" },
            {"by group", "dbpartition by hash(id)", ""},
            {"by group", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
            {"by group", "tbpartition by hash(id) tbpartitions 2", ""},
            {"by group", "broadcast", ""},
            {"by time", "dbpartition by hash(id)", ""},
            {"by time", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2", ""},
            {"by time", "tbpartition by hash(id) tbpartitions 2", ""},
            {"by time", "broadcast", ""},
            {"", "", PropertiesUtil.polardbXShardingDBName2()},
            {"", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {
                "", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
                PropertiesUtil.polardbXShardingDBName2()},
            {"", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            {"", "broadcast", PropertiesUtil.polardbXShardingDBName2()},
            {"by simple", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {
                "by simple", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
                PropertiesUtil.polardbXShardingDBName2()},
            {"by simple", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            {"by simple", "broadcast", PropertiesUtil.polardbXShardingDBName2()},
            //{ "by simple with cache", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2() },
            //{ "by simple with cache", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
            //        PropertiesUtil.polardbXShardingDBName2() },
            //{ "by simple with cache", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2() },
            //{ "by simple with cache", "broadcast", PropertiesUtil.polardbXShardingDBName2() },
            {"by group", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {
                "by group", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
                PropertiesUtil.polardbXShardingDBName2()},
            {"by group", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            {"by group", "broadcast", PropertiesUtil.polardbXShardingDBName2()},
            {"by time", "dbpartition by hash(id)", PropertiesUtil.polardbXShardingDBName2()},
            {
                "by time", "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
                PropertiesUtil.polardbXShardingDBName2()},
            {"by time", "tbpartition by hash(id) tbpartitions 2", PropertiesUtil.polardbXShardingDBName2()},
            {"by time", "broadcast", PropertiesUtil.polardbXShardingDBName2()}};
        // String[][] postFix = {{""}};
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

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqAutoIncrementStartWith() throws Exception {

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

        if (!isSpecialSequence(seqType)) {
            assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
                "auto_id",
                tddlConnection)).contains(40L);
        }
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqAutoIncrementStartWith2() throws Exception {
        String sql = String.format(
            "create table %s (auto_id int not null auto_increment primary key, id int , name varchar(20)) dbpartition by hash(id)",

            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s auto_increment = 40", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String insert_sql = String.format("insert into %s (id, name) values (1, 'abc')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert_sql);

        if (!isSpecialSequence(seqType)) {
            assertThat(JdbcUtil.selectIds(String.format("select auto_id from  %s order by auto_id", tableName),
                "auto_id",
                tddlConnection)).contains(200001L);
        }
    }
}
