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

package com.alibaba.polardbx.qatest.failpoint.base;

import com.alibaba.polardbx.qatest.ddl.auto.dag.BaseDdlEngineTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class DdlGraphVizGenerator extends BaseDdlEngineTestCase {

    @Test
    public void test0() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists t1");

        showDDLGraph(
            "CREATE TABLE `t1` (\n"
                + "\t`c1` bigint(20) NOT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\tPRIMARY KEY USING BTREE (`c1`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`c1`) tbpartition by hash(c1) tbpartitions 3");
        System.out.println("CREATE TABLE");

        showDDLGraph("alter table t1 add column c4 bigint");
        System.out.println("ALTER TABLE");

        showDDLGraph("rename table t1 to t2");
        System.out.println("RENAME TABLE");
        showDDLGraph("rename table t2 to t1");

        showDDLGraph("truncate table t1");
        System.out.println("TRUNCATE TABLE");

        showDDLGraph("create index idx1 on t1(c4)");
        System.out.println("CREATE INDEX");

        showDDLGraph("drop index idx1 on t1");
        System.out.println("DROP INDEX");

        showDDLGraph(
            "alter table t1 add global INDEX `gsi1a`(`c2`) DBPARTITION BY HASH(`c2`) tbpartition by hash(c2) tbpartitions 3");
        System.out.println("CREATE GSI");

        showDDLGraph("drop index gsi1a on t1");
        System.out.println("DROP GSI");

        showDDLGraph("ALTER TABLE t1 broadcast");
        System.out.println("REPARTITION");

        showDDLGraph("drop table t1");
        System.out.println("DROP TABLE");

        showDDLGraph("CREATE TABLE `t1` (\n"
            + "\t`c1` bigint(20) NOT NULL,\n"
            + "\t`c2` bigint(20) DEFAULT NULL,\n"
            + "\t`c3` bigint(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY USING BTREE (`c1`),\n"
            + "\tglobal INDEX `gsi1a`(`c2`) DBPARTITION BY HASH(`c2`) tbpartition by hash(c2) tbpartitions 3,\n"
            + "\tglobal INDEX `gsi2a`(`c2`) DBPARTITION BY HASH(`c2`) tbpartition by hash(c2) tbpartitions 3,\n"
            + "\tglobal INDEX `gsi3a`(`c2`) DBPARTITION BY HASH(`c2`) tbpartition by hash(c2) tbpartitions 3\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`c1`) tbpartition by hash(c1) tbpartitions 3");
        System.out.println("CREATE TABLE WITH GSI");

        showDDLGraph("drop table t1");
        System.out.println("DROP TABLE WITH GSI");
    }

}
