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

package com.alibaba.polardbx.qatest.failpoint.recoverable.legacy;

import com.alibaba.polardbx.qatest.failpoint.base.BaseTableFailPointTestCase;
import org.junit.Assert;

public class CreateDropIndexRecoverableTest extends BaseTableFailPointTestCase {

    protected void testSingleTable() {
        String tableName = createAndCheckTable("index_single_table", EXTENDED_SINGLE);
        testIndexOperations(tableName);
    }

    protected void testBroadcastTable() {
        String tableName = createAndCheckTable("index_broadcast_table", EXTENDED_BROADCAST);
        testIndexOperations(tableName);
    }

    protected void testShardingTable() {
        String tableName = createAndCheckTable("index_sharding_table", EXTENDED_SHARDING);
        testIndexOperations(tableName);
    }

    protected void testIndexOperations(String tableName) {
        // Create Index
        String indexName = "idxAdded";
        String ddl = String.format("create index %s on %s(c_bigint)", indexName, tableName);
        executeAndCheckTableWithIndex(ddl, tableName, indexName, true);

        // Drop Index
        ddl = String.format("drop index %s on %s", indexName, tableName);
        executeAndCheckTableWithIndex(ddl, tableName, indexName, false);
    }

    protected void executeAndCheckTableWithIndex(String stmt, String tableName, String indexName,
                                                 boolean checkIfExists) {
        executeAndCheckTable(stmt, tableName);
        boolean exists = hasIndex(tableName, indexName);
        Assert.assertTrue(checkIfExists ? exists : !exists);
    }

}
