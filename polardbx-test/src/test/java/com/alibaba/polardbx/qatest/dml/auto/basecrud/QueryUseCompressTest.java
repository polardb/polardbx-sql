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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepareWithBatch;
import static com.google.common.truth.Truth.assertWithMessage;

public class QueryUseCompressTest extends AutoCrudBasedLockTestCase {
    private static final Log log = LogFactory.getLog(QueryUseCompressTest.class);
    /**
     * 这个数据量下，使用压缩模式时，全量查询返回的结果在未压缩前已经超过1<<24B（临界值），触发了packet的拆分
     */
    public static final int DATA_COUNTS = 250000;

    public QueryUseCompressTest() {
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX;
    }

    @Before
    public void initData() throws Exception {
//        System.out.println("开始执行插入");
        tableDataPrepareWithBatch(baseOneTableName, 0, DATA_COUNTS,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "SELECT count(*) FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
//        System.out.println("开始执行查询");
    }

    /**
     * 使用compress协议，全量查询，返回数据量大，测试返回结果正确性
     */
    @Test
    public void selectAll() throws Exception {

        // 查询语句
        String sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssertWithoutPrint(sql, null, mysqlConnection,
            tddlConnection, DATA_COUNTS);
    }

    /**
     * 结果集较大，不适合打印结果
     * 重写selectContentSameAssert方法
     */
    public void selectContentSameAssertWithoutPrint(String sql, List<Object> param, Connection mysqlConnection,
                                                    Connection tddlConnection, int count) {

        ResultSet mysqlRs = null;
        PreparedStatement mysqlPs = null;
        ResultSet tddlRs = null;
        PreparedStatement tddlPs = null;

        try {
            mysqlPs = JdbcUtil.preparedStatementSet(sql, param, mysqlConnection);
            mysqlRs = JdbcUtil.executeQuery(sql, mysqlPs);

            if (mysqlRs.getWarnings() != null) {
                String warningMessage = mysqlRs.getWarnings().getMessage();
                if ((!StringUtils.isEmpty(warningMessage)) && warningMessage.contains("Incorrect") && warningMessage
                    .contains("value") && warningMessage.contains("for column")) {
                    return;
                }
            }
            tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
            tddlRs = JdbcUtil.executeQuery(sql, tddlPs);

            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs, false);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs, false);

//            System.out.println("mysqlResult rows: " + mysqlResults.size());
//            System.out.println("tddlResult rows: " + tddlResults.size());
            // 不允许为空结果集合

            Assert.assertTrue("sql语句:" + sql + " 查询的结果集为空，请修改sql语句，保证有结果集", mysqlResults.size() != 0);

            assertWithMessage(" 非顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + sql + " 参数为 :" + param).that(mysqlResults)
                .hasSize(tddlResults.size());

        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlPs);
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
        }
    }
}
