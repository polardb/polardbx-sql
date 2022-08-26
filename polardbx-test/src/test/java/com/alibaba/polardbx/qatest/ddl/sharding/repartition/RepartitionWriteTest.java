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

package com.alibaba.polardbx.qatest.ddl.sharding.repartition;

import com.alibaba.polardbx.qatest.constant.TableConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

import static org.hamcrest.Matchers.is;

/**
 * 拆分键变更测试 - 主要测试增量多写逻辑
 * <p>
 * 测试方法：使用hint将状态停留在多写状态，使用trace查看执行计划是否复合预期
 * <p>
 * -----
 * 以方法定义顺序执行测试用例
 *
 * @author guxu
 */
@FixMethodOrder(value = MethodSorters.JVM)

public class RepartitionWriteTest extends RepartitionBaseTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
        initDatasourceInfomation();
    }

    @Parameterized.Parameters(name = "{index}:primaryTableName={0}")
    public static List<String> prepareDate() {
        return Lists.newArrayList(DEFAULT_PRIMARY_TABLE_NAME, MULTI_PK_PRIMARY_TABLE_NAME);
    }

    public RepartitionWriteTest(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    @Test
    public void s2pThenStayAtDeleteOnly() throws SQLException {
        //模拟拆分表变更执行到DeleteOnly的状态
        final String hint =
            " /*+TDDL:CMD_EXTRA(GSI_FINAL_STATUS_DEBUG=DELETE_ONLY,REPARTITION_SKIP_CUTOVER=true,REPARTITION_SKIP_CLEANUP=true) */";

        executeDDL(String.format("CREATE SEQUENCE AUTO_SEQ_%s", primaryTableName));

        executeSimpleTestCase(
            primaryTableName,
            ruleOf("SINGLE"),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3),
            hint,
            true
        );

        //因为是delete only状态，所以预期只写主表
        String sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));

        //因为是delete only状态，所以预期GSI表包含DELETE流量
        sql = MessageFormat.format("UPDATE {0} SET c_timestamp = NOW() WHERE c_int_32 = 1", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 1);

        //因为是delete only状态，所以预期GSI表和主表都包含DELETE流量
        sql = MessageFormat.format("DELETE FROM {0} WHERE c_int_32 = 1", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //预期：select + insert
        sql = MessageFormat
            .format("INSERT IGNORE INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //预期：select + insert
        sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now()) ON DUPLICATE KEY UPDATE c_int_32=2", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //预期：select + insert
        sql = MessageFormat
            .format("REPLACE INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (2, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //预期：select + delete + insert
        sql = MessageFormat
            .format("INSERT IGNORE INTO {0} (id, c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (232, 3, now(), now(), now(), now())", primaryTableName);
        executeDml(dmlHintStr + sql);
        sql = MessageFormat
            .format("REPLACE INTO {0} (id, c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (232, 3, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));
        assertTraceContains(trace, "DELETE", 1);
    }

    @Test
    public void s2pThenStayAtWriteOnly() throws SQLException {
        //模拟拆分表变更执行到DeleteOnly的状态
        final String hint =
            " /*+TDDL:CMD_EXTRA(GSI_FINAL_STATUS_DEBUG=WRITE_ONLY,REPARTITION_SKIP_CUTOVER=true,REPARTITION_SKIP_CLEANUP=true) */";

        executeDDL(String.format("CREATE SEQUENCE AUTO_SEQ_%s", primaryTableName));

        executeSimpleTestCase(
            primaryTableName,
            ruleOf("SINGLE"),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3),
            hint,
            true
        );

        //因为是write only状态，所以预期写主表和GSI表
        String sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //预期：select + update + update
        sql = MessageFormat.format("UPDATE {0} SET c_timestamp = NOW() WHERE c_int_32 = 1", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //因为是write only状态，所以预期GSI表和主表都包含DELETE流量
        sql = MessageFormat.format("DELETE FROM {0} WHERE c_int_32 = 1", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //预期：select + insert + insert
        sql = MessageFormat
            .format("INSERT IGNORE INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + insert + insert
        sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now()) ON DUPLICATE KEY UPDATE c_int_32=2", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + replace + replace
        sql = MessageFormat
            .format("REPLACE INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (2, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + replace + delete + insert
        sql = MessageFormat
            .format("INSERT IGNORE INTO {0} (id, c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (232, 3, now(), now(), now(), now())", primaryTableName);
        executeDml(dmlHintStr + sql);
        sql = MessageFormat
            .format("REPLACE INTO {0} (id, c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (232, 3, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(4));
        assertTraceContains(trace, "DELETE", 1);
        assertTraceContains(trace, "REPLACE", 1);
    }

}
