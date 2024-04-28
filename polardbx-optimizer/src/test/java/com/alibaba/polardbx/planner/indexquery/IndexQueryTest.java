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

package com.alibaba.polardbx.planner.indexquery;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Random;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DATA_MAX_LEN;
import static org.junit.Assert.assertEquals;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class IndexQueryTest extends IndexTestCommon {

    public IndexQueryTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(IndexQueryTest.class);
    }

    @Test
    public void testStatisticManagerTruncate() {
        OptimizerContext context = getContextByAppName(appName);
        for (TableMeta tableMeta : context.getLatestSchemaManager().getAllTables()) {
            for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
                StatisticManager statisticManager = StatisticManager.getInstance();
                DataType columnRealType =
                    statisticManager.getRealDataType(appName, tableMeta.getTableName(), columnMeta.getName());
                Integer originalLen = new Random().nextInt(255);
                String s = RandomStringUtils.random(originalLen);
                int truncatedLen =
                    statisticManager.truncateStringTypeValue(appName, tableMeta.getTableName(), columnMeta.getName(), s)
                        .length();
                if (DataTypeUtil.isStringType(columnRealType)) {
                    assertEquals("targetLen should equals originalLen ", truncatedLen,
                        Math.min((long) originalLen, DATA_MAX_LEN));
                } else {
                    assertEquals("targetLen should equals originalLen ", truncatedLen, (long) originalLen);
                }
            }
        }
    }
}
