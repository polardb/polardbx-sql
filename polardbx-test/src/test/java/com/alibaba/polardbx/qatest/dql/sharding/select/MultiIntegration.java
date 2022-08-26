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

package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.common.utils.thread.ExecutorTemplate;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 复杂子查询测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class MultiIntegration extends ReadBaseTestCase {

    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect
            .selectBaseOneBaseTwoMutilDbMutilTb());
    }

    public MultiIntegration(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testCorrelated() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
            .newCachedThreadPool();
        ExecutorTemplate template = new ExecutorTemplate(executor);

        int threadCount = 5;
        for (int i = 0; i < threadCount; i++) {
            template.submit(new Runnable() {

                @Override
                public void run() {
                    Connection mysqlConnection = null;
                    Connection tddlConnection = null;

                    mysqlConnection = getMysqlConnection();
                    tddlConnection = getPolardbxConnection();

                    for (int i = 0; i < 10; i++) {
                        String sql = "select * from " + baseOneTableName
                            + " as host where pk = (select pk from "
                            + baseTwoTableName
                            + " as info where host.integer_test=info.pk )";
                        selectContentSameAssert(sql, null,
                            mysqlConnection, tddlConnection, true);
                    }

                    JdbcUtil.close(tddlConnection);
                    JdbcUtil.close(mysqlConnection);
                }
            });
        }
        template.waitForResult();
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testSubquery() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
            .newCachedThreadPool();
        ExecutorTemplate template = new ExecutorTemplate(executor);

        int threadCount = 5;
        for (int i = 0; i < threadCount; i++) {
            template.submit(new Runnable() {

                @Override
                public void run() {
                    Connection mysqlConnection = null;
                    Connection tddlConnection = null;
                    mysqlConnection = getMysqlConnection();
                    tddlConnection = getPolardbxConnection();
                    for (int i = 0; i < 50; i++) {
                        String sql = "select *  from " + baseOneTableName
                            + " where integer_test <(select pk from "
                            + baseTwoTableName + " where varchar_test=? order by pk limit 1)";
                        List<Object> param = new ArrayList<Object>();
                        param.add(columnDataGenerator.varchar_testValue);
                        selectContentSameAssert(sql, param,
                            mysqlConnection, tddlConnection, true);
                    }

                    JdbcUtil.close(tddlConnection);
                    JdbcUtil.close(mysqlConnection);
                }
            });
        }
        template.waitForResult();
    }
}
