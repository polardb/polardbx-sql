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

package com.alibaba.polardbx.qatest.failpoint.newpartition.failpoint;

import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;

import java.sql.ResultSet;
import java.util.Random;

import static com.alibaba.polardbx.qatest.validator.DataValidator.resultSetContentSameAssert;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */

public class AlterTableGroupMovePartitionTest extends BaseAlterTableGroupFailPointTestCase {

    public AlterTableGroupMovePartitionTest(Pair<PartitionStrategy, String[]> statements) {
        createTableStats = statements;
    }

    @Override
    protected void execDdlWithFailPoints() {
        final ResultSet beforeReorg = JdbcUtil.executeQuerySuccess(failPointConnection, unionAllTables);
        Random r = new Random();
        int val = r.nextInt(2);
        switch (createTableStats.getKey()) {
        case KEY:
        case HASH:
        case RANGE:
        case LIST:
            if (val == 1) {
                JdbcUtil
                    .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName
                        + " move partitions p5,p1,p2,p3,p4 to 'polardbx-storage-0-master'");
            } else {
                JdbcUtil
                    .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName
                        + " move partitions p5,p1,p2,p3,p4 to 'polardbx-storage-1-master'");
            }
            break;
        }
        final ResultSet afterReorg = JdbcUtil.executeQuerySuccess(failPointConnection, unionAllTables);
        resultSetContentSameAssert(beforeReorg, afterReorg, false);
    }
}
