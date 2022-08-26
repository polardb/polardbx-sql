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

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * Test subjob operations
 *
 * @author moyi
 * @since 2021/11
 */
@Ignore
public class SubJobTest extends DDLBaseNewDBTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SubJobTest.class);

    private void enableMockDdl() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("set @%s=%s", FailPointKey.FP_HIJACK_DDL_JOB, "'10,5,30'"));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("set @%s=%s", FailPointKey.FP_INJECT_SUBJOB, "'true'"));
    }

    private void disableMockDdl() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("set @%s=''", FailPointKey.FP_HIJACK_DDL_JOB));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("set @%s=%s", FailPointKey.FP_INJECT_SUBJOB, "''"));
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        enableMockDdl();
    }

    @After
    public void after() {
        disableMockDdl();
    }

    @Test
    public void testSubJob() throws SQLException {

        // create a job with subjob
        long jobId = 0;
        long subJobId = 0;
        String asyncHint = "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=true,PURE_ASYNC_DDL_MODE=true)*/";
        JdbcUtil.executeUpdateSuccess(tddlConnection, asyncHint + " create table hehe(id int)");

        // show ddl and show full ddl
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show ddl")) {
            List<List<Object>> rows = JdbcUtil.getAllResult(rs);
            Assert.assertEquals(1, rows.size());
            jobId = DataTypes.LongType.convertFrom(rows.get(0).get(0));
        }
        LOG.info("create a job which jobId=" + jobId);

        // pause the job will pause the subjob
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("pause ddl %d", jobId));

        // get subjob
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show full ddl")) {
            List<List<Object>> rows = JdbcUtil.getAllResult(rs);
            Assert.assertTrue(rows.size() >= 2);
            Optional<List<Object>> subjobRow =
                rows.stream().filter(row -> row.get(15).toString().startsWith("subjob")).findFirst();
            Assert.assertTrue(subjobRow.isPresent());
            subJobId = DataTypes.LongType.convertFrom(subjobRow.get().get(0));
        }

        // recover/rollback subjob is not allowed;
        String rollbackSub = String.format("rollback ddl %d", subJobId);
        String recoverSub = String.format("recover ddl %d", subJobId);
        JdbcUtil.executeUpdateFailed(tddlConnection, rollbackSub, "Operation on subjob is not allowed");
        JdbcUtil.executeUpdateFailed(tddlConnection, recoverSub, "Operation on subjob is not allowed");

        // recover parent job
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("recover ddl %d", jobId));
    }
}
