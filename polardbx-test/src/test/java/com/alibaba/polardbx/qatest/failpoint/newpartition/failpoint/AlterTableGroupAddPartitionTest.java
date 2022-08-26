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

import static com.alibaba.polardbx.qatest.validator.DataValidator.resultSetContentSameAssert;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */

public class AlterTableGroupAddPartitionTest extends BaseAlterTableGroupFailPointTestCase {

    public AlterTableGroupAddPartitionTest(Pair<PartitionStrategy, String[]> statements) {
        createTableStats = statements;
    }

    @Override
    protected void execDdlWithFailPoints() {
        final ResultSet beforeReorg = JdbcUtil.executeQuerySuccess(failPointConnection, unionAllTables);
        switch (createTableStats.getKey()) {
        case KEY:
        case HASH:
            JdbcUtil
                .executeUpdateFailed(failPointConnection, "alter tablegroup " + tgName
                    + " add partition (partition p99 values less than(10),partition p100 values less than(20))", "");
            return;
        case RANGE:
            JdbcUtil
                .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName
                    + " add partition (partition p99 values less than(200000),partition p100 values less than(300000))");
            break;
        case LIST:
            JdbcUtil
                .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName
                    + " add partition (partition p99 values in (200000,200001,200002),partition p100 values in (300000,300001,300002))");
            break;
        }
        final ResultSet afterReorg = JdbcUtil.executeQuerySuccess(failPointConnection, unionAllTables);
        resultSetContentSameAssert(beforeReorg, afterReorg, false);
    }
}
