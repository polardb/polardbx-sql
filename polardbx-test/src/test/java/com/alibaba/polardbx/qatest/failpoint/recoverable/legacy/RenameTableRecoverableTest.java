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

public class RenameTableRecoverableTest extends BaseTableFailPointTestCase {

    protected void testSingleTable() {
        String tableName = createAndCheckTable("rename_single_table", EXTENDED_SINGLE);
        testRenameTable(tableName);
    }

    protected void testBroadcastTable() {
        String tableName = createAndCheckTable("rename_broadcast_table", EXTENDED_BROADCAST);
        testRenameTable(tableName);
    }

    protected void testShardingTable() {
        String tableName = createAndCheckTable("rename_sharding_table", EXTENDED_SHARDING);
        testRenameTable(tableName);
    }

    protected void testRenameTable(String tableName) {
        String newTableName = tableName + "_new";
        String ddl = String.format("rename table %s to %s", tableName, newTableName);
        executeAndCheckTable(ddl, newTableName);
    }

}
