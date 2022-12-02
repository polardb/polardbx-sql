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
import com.alibaba.polardbx.qatest.entity.TestSequence;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * Created by xiaowen.guoxw on 16-6-16.
 */

public class DropSequenceTest extends DDLBaseNewDBTestCase {

    public DropSequenceTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testDropTableWithDropOldSequence() throws Exception {
        String tableName = schemaPrefix + "old_seq";
        dropTableIfExists(tableName);

        String seqName = schemaPrefix + "AUTO_SEQ_old_seq";

        String sql = "create table " + tableName + " (id int, name varchar(20))dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        Assert.assertNotNull(sequence);

        dropTableIfExists(tableName);

        sequence = showSequence(seqName);
        Assert.assertNull(sequence);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testDropAUTOSequence() throws Exception {
        String seqName = schemaPrefix + "AUTO_SEQ_seq";
        dropSequence(seqName);
        String sql = String.format("create sequence %s", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        Assert.assertNotNull(sequence);

        dropSequence(seqName);

        sequence = showSequence(seqName);
        Assert.assertNull(sequence);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testDropTableAUTOSequence() throws Exception {
        String simpleTableName = "drop_table_test_2_2017";
        String tableName = schemaPrefix + simpleTableName;
        String seqName = schemaPrefix + "AUTO_SEQ_" + simpleTableName;
        dropTableIfExists(tableName);
        String sql = String
            .format("create table %s (id int auto_increment primary key, name varchar(20))dbpartition by hash(id) ",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(name) values('1')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        Assert.assertNotNull(sequence);

        dropTableIfExists(tableName);
        sequence = showSequence(seqName);
        Assert.assertNull(sequence);

    }

}
