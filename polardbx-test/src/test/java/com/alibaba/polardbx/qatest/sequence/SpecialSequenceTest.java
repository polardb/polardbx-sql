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
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by xiaowen.guoxw on 16-11-29.
 * time与group sequence的处理与判断方式与其他sequence不同,这里主要补充这两种sequence的测试用例
 */
@NotThreadSafe

public class SpecialSequenceTest extends BaseSequenceTestCase {

    public SpecialSequenceTest(String schema) {
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
    }

    @Parameterized.Parameters(name = "{index}:schema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {{""}, {PropertiesUtil.polardbXShardingDBName2()}});
    }

    /**
     * 正常创建time sequence的方法
     */
    @Test
    public void testNormalTimedSequence() throws Exception {
        String simpleTableName = "time_seq_test";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (id bigint primary key auto_increment by time, name varchar(20)) dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        simpleCheckSequence(schemaPrefix + "AUTO_SEQ_" + simpleTableName, "time");

        sql = String.format(
            "insert into %s (id, name) values (0, 'a'), (null, 'b'),(100, 'd'), (0, 'e'), ('200', 'f'), (0, 'd')",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";

        assertThat(JdbcUtil.selectIds(sql, "ID", tddlConnection)).containsAnyOf(100L, 200L);
        assertThat(JdbcUtil.selectIds(sql, "ID", tddlConnection)).containsNoneOf(101L, 201L);
        dropTableIfExists(tableName);

    }

    /**
     * 正常创建group sequence的方法
     */
    @Test
    public void testNormalGroupSequence() throws Exception {
        String simpleTableName = "group_seq_test";
        String tableName = schemaPrefix + simpleTableName;

        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (id bigint primary key auto_increment by group, name varchar(20)) dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        simpleCheckSequence(schemaPrefix + "AUTO_SEQ_" + simpleTableName, "group");

        sql = String.format(
            "insert into %s (id, name) values (0, 'a'), (null, 'b'),(100, 'd'), (0, 'e'), ('200', 'f'), (0, 'd')",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select ID from " + tableName + " order by ID";

        assertThat(JdbcUtil.selectIds(sql, "ID", tddlConnection)).containsAnyOf(100L, 200L);
        assertThat(JdbcUtil.selectIds(sql, "ID", tddlConnection)).containsNoneOf(101L, 201L);
        dropTableIfExists(tableName);

    }

}
