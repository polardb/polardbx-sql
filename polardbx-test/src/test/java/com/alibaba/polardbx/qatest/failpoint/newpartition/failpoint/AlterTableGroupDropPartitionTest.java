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

import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;

import java.sql.ResultSet;

import static com.alibaba.polardbx.qatest.validator.DataValidator.resultSetContentSameAssert;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */

public class AlterTableGroupDropPartitionTest extends BaseAlterTableGroupFailPointTestCase {
    protected static String unionPartialPartitions =
        "select * from (select 1 as col, t1.* from " + tb1 + " partition(p8,p1,p3,p6,p7) t1 union all "
            + "select 2 as col, t2.* from " + tb2
            + " partition(p8,p1,p3,p6,p7) t2 union all "
            + "select 3 as col, t3.* from " + tb3
            + " partition(p8,p1,p3,p6,p7) t3 union all " + "select 4 as col, t4.* from " + tb4
            + " partition(p8,p1,p3,p6,p7) t4) a order by col, id";

    public AlterTableGroupDropPartitionTest(Pair<PartitionStrategy, String[]> statements) {
        createTableStats = statements;
    }

    @Override
    protected void execDdlWithFailPoints() {
        final ResultSet beforeReorg = JdbcUtil.executeQuerySuccess(failPointConnection, unionPartialPartitions);

        switch (createTableStats.getKey()) {
        case KEY:
        case HASH:
            JdbcUtil
                .executeUpdateFailed(failPointConnection, "alter tablegroup " + tgName
                    + " drop partition p2,p4,p5", "");
            return;
        case RANGE:
        case LIST:
            JdbcUtil
                .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName
                    + " drop partition p2,p4,p5");
            break;
        }
        final ResultSet afterReorg = JdbcUtil.executeQuerySuccess(failPointConnection, unionAllTables);
        resultSetContentSameAssert(beforeReorg, afterReorg, false);
    }
}
