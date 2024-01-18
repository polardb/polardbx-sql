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
import java.util.Random;

import static com.alibaba.polardbx.qatest.validator.DataValidator.resultSetContentSameAssert;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */

public class AlterTableGroupSplitPartitionTest extends BaseAlterTableGroupFailPointTestCase {

    public AlterTableGroupSplitPartitionTest(Pair<PartitionStrategy, String[]> statements) {
        createTableStats = statements;
    }

    @Override
    protected void execDdlWithFailPoints() {
        Random r = new Random();
        int val = r.nextInt(2);

        final ResultSet beforeReorg = JdbcUtil.executeQuerySuccess(failPointConnection, unionAllTables);

        switch (createTableStats.getKey()) {
        case KEY:
            JdbcUtil
                .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName + " split partition p1");
            break;
        case HASH:
            if (val == 1) {
                JdbcUtil
                    .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName + " split partition p1");
            } else {
                JdbcUtil
                    .executeUpdateSuccess(failPointConnection,
                        "alter tablegroup " + tgName + " extract to partition by hot value(100010)");
            }
            break;
        case RANGE:
            if (val == 1) {
                JdbcUtil
                    .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName
                        + " split partition p2 at(100050) into (partition p11, partition p12) ");
            } else {
                JdbcUtil
                    .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName
                        + " split partition p2 into (partition p11 values less than(100060), partition p12 values less than(100080)) ");
            }
            break;
        case LIST:
            JdbcUtil
                .executeUpdateSuccess(failPointConnection, "alter tablegroup " + tgName
                    + " split partition p2 into (partition p11 values in (100010,100011,100012,100013,100014), partition p12 values in (100015,100016,100017,100018,100019))");
            break;
        }
        final ResultSet afterReorg = JdbcUtil.executeQuerySuccess(failPointConnection, unionAllTables);
        resultSetContentSameAssert(beforeReorg, afterReorg, false);

    }
}
