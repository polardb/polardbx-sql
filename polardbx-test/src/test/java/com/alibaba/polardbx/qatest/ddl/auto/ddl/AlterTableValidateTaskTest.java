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

package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.ddl.auto.autoNewPartition.BaseAutoPartitionNewPartition;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

public class AlterTableValidateTaskTest extends BaseAutoPartitionNewPartition {

    @Test
    public void testAlterTableAddPrimaryNew() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 add primary index (a)", "not supported");
    }

    @Test
    public void testAlterTableDropPrimaryNew() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table mengshi1(a int,b char, primary key(a))");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 drop primary key", "not supported");
    }

    @Test
    public void testAlterTableDropPartitionKeyNewPartition() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table mengshi1(a int,b char) partition by hash(a) partitions 2");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 drop column a", "not supported");

        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table mengshi1(a int,b char) partition by key(a,b) partitions 2");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table mengshi1 drop column a", "not supported");
    }

    @Test
    public void testAlterTableModifyPartitionKeyNewPartition() {
        dropTableIfExists(tddlConnection, "mengshi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table mengshi1(a int,b char) partition by hash(a) partitions 16");
        JdbcUtil.executeUpdateFailed(tddlConnection, "alter table mengshi1 modify column a int", "not supported");
    }

    @Test
    public void testAlterTableModifyAfter() {
        dropTableIfExists(tddlConnection, "wumu1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table wumu1(a int,b int, c char, d int) partition by hash(a) partitions 2");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table wumu1 add global index `idx`(a, b) partition by hash(a) partitions 2");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table wumu1 modify column b int after c");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table wumu1 modify column b char after c", "not recommended");
    }

}
