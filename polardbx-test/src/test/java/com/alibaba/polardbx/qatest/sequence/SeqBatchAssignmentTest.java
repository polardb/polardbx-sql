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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * Created by chensr on 2018-02-02 测试批量分配 sequence 值的正确性，确保 group sequence
 * 避免被"优化"而遇到超过 innerStep 报错的情况
 */

public class SeqBatchAssignmentTest extends BaseSequenceTestCase {

    private String shardingDestWithGroup;
    private String shardingDestWithSimple;
    private String singleSrcWithoutSeq;

    private static final String insertSelectStmt = "insert into %s(c2) select c2 from %s";
    private static String insertMultiValuesStmt = "insert into %s(c2) values (1)";

    public SeqBatchAssignmentTest(String schema) {
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.shardingDestWithGroup = schemaPrefix + randomTableName("shardingDestWithGroup5", 4);
        this.shardingDestWithSimple = schemaPrefix + randomTableName("shardingDestWithSimple6", 4);
        this.singleSrcWithoutSeq = schemaPrefix + randomTableName("singleSrcWithoutSeq7", 4);
    }

    @Parameterized.Parameters(name = "{index}:schema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {{""}, {PropertiesUtil.polardbXShardingDBName2()}});
    }

    static {
        for (int i = 0; i < 15; i++) {
            insertMultiValuesStmt += ",(1)";
        }
    }

    @Before
    public void init() {
        String sqlCreateGroup =
            "create table if not exists %s (c1 int auto_increment unit count 1 index 0 step %s, c2 int, primary key (c1)) dbpartition by hash(c1)";
        String sqlCreateSimple =
            "create table if not exists %s (c1 int auto_increment by %s, c2 int,primary key (c1)) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateGroup, shardingDestWithGroup, "100"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateSimple, shardingDestWithSimple, "simple"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateGroup, singleSrcWithoutSeq, "100"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertMultiValuesStmt, singleSrcWithoutSeq));
    }

    @After
    public void destroy() {
        dropTableIfExists(singleSrcWithoutSeq);
        dropTableIfExists(shardingDestWithSimple);
        dropTableIfExists(shardingDestWithGroup);
    }

    // Inserting multiple values still has the limitation of the innerStep.
    @Ignore
    @Test
    public void testInsertMultiValuesIntoGroup() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertMultiValuesStmt, shardingDestWithGroup));
    }

    @Test
    public void testInsertMultiValuesIntoSimple() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertMultiValuesStmt, shardingDestWithSimple));
    }

    @Test
    public void testInsertSelectIntoGroup() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(insertSelectStmt, shardingDestWithGroup, singleSrcWithoutSeq));
    }

    @Test
    public void testInsertSelectIntoSimple() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(insertSelectStmt, shardingDestWithSimple, singleSrcWithoutSeq));
    }

}
